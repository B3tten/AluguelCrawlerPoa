import json
import os
import re
import subprocess
import sys
import threading
import time
from datetime import datetime
from pathlib import Path
from urllib.parse import urlparse

import redis as redis_lib
import requests
from flask import Flask, Response, abort, jsonify, render_template, request
from pymongo import MongoClient

BASE_DIR = Path(__file__).resolve().parent.parent

CITY = os.environ.get('CRAWLER_CITY', 'Porto Alegre')
CITY_CENTER = [float(os.environ.get('CRAWLER_LAT', '-30.0346')),
               float(os.environ.get('CRAWLER_LNG', '-51.2177'))]

MONGO_URI = os.environ.get('MONGODB_URI', 'mongodb://root:pass@localhost:27017')
MONGO_DATABASE = os.environ.get('MONGODB_DATABASE', 'rent')
REDIS_HOST = os.environ.get('REDIS_HOST', 'localhost')
REDIS_PORT = int(os.environ.get('REDIS_PORT', 6379))

SOURCES = [
    ('zap', 'ZAP Imóveis', 'data/zap.json'),
    ('quinto_andar', 'QuintoAndar', 'data/quinto_andar.json'),
]

app = Flask(__name__)

client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=3000)

CRAWL_STATE = {
    'running': False,
    'spider': None,
    'message': 'Pronto',
    'started_at': None,
    'finished_at': None,
    'results': {},
    'error': None,
}
CRAWL_LOCK = threading.Lock()


# ---------------------------------------------------------------- listings ---
def _num(value):
    if value is None or isinstance(value, bool):
        return None
    try:
        return round(float(value), 2)
    except (TypeError, ValueError):
        return None


def _first(value):
    if isinstance(value, list):
        return value[0] if value else None
    return value


def _is_url(value):
    return isinstance(value, str) and value.startswith('http')


def _base_listing(source, source_label, doc):
    address = doc.get('address') or {}
    details = doc.get('details') or {}
    text_details = doc.get('text_details') or {}

    return {
        'source': source,
        'source_label': source_label,
        'code': str(doc.get('code') or ''),
        'url': doc.get('url'),
        'neighbourhood': address.get('neighbourhood'),
        'street': address.get('street'),
        'street_number': address.get('street_number'),
        'zone': address.get('zone'),
        'city': address.get('city'),
        'lat': _num(address.get('lat')),
        'lng': _num(address.get('lng') or address.get('lon')),
        'area': _num(_first(details.get('area'))),
        'bedrooms': _num(_first(details.get('bedrooms'))),
        'bathrooms': _num(_first(details.get('bathrooms'))),
        'parking': _num(_first(details.get('parking'))),
        'suites': _num(_first(details.get('suites'))),
        'property_type': _first(details.get('type')),
        'features': [str(f) for f in (text_details.get('features') or [])][:12],
        'description': text_details.get('description') or text_details.get('owner_description'),
        'scrapped_at': doc['scrapped_at'].isoformat() if isinstance(doc.get('scrapped_at'), datetime) else None,
    }


TYPE_LABELS = {
    'APARTMENT': 'Apartamento',
    'HOME': 'Casa',
    'CONDOMINIUM': 'Condomínio',
    'PENTHOUSE': 'Cobertura',
    'FLAT': 'Flat',
    'KITNET': 'Kitnet',
    'LOFT': 'Loft',
    'TWO_STORY_HOUSE': 'Sobrado',
    'RESIDENTIAL_BUILDING': 'Prédio residencial',
    'VILLAGE_HOUSE': 'Casa em vila',
}


def _zap_title(listing, text_details):
    title = (text_details.get('title') or '').strip()
    if title and title.upper() not in TYPE_LABELS:
        return title

    property_type = str(listing.get('property_type') or '').upper()
    label = TYPE_LABELS.get(property_type, 'Imóvel')
    specs = []
    if listing.get('bedrooms'):
        specs.append(f"{int(listing['bedrooms'])} quarto{'s' if listing['bedrooms'] > 1 else ''}")
    if listing.get('area'):
        specs.append(f"{int(listing['area'])} m²")
    return ' · '.join([label] + specs)


def _zap_listing(doc):
    listing = _base_listing('zap', 'ZAP Imóveis', doc)

    prices = doc.get('prices') or []
    price = next(
        (p for p in prices if str(p.get('type', '')).upper().endswith('RENTAL')),
        prices[0] if prices else {},
    )
    listing.update({
        'rent': _num(price.get('rent')),
        'condo': _num(price.get('condominium')),
        'iptu': _num(price.get('iptu')),
        'total': _num(price.get('total')),
    })

    media = doc.get('media') or {}
    images = [url.replace('{description}', '1') for url in (media.get('images') or []) if _is_url(url)]
    listing['images'] = images
    listing['image'] = images[0] if images else None

    text_details = doc.get('text_details') or {}
    listing['title'] = _zap_title(listing, text_details)
    return listing


def _quinto_andar_listing(doc):
    listing = _base_listing('quinto_andar', 'QuintoAndar', doc)

    prices = doc.get('prices') or {}
    listing.update({
        'rent': _num(prices.get('rent')),
        'condo': _num(prices.get('condominium')),
        'iptu': _num(prices.get('iptu')),
        'total': _num(prices.get('total')),
    })

    media = doc.get('media') or {}
    images = []
    for image in media.get('images') or []:
        if isinstance(image, dict):
            images.extend(url for url in image.values() if _is_url(url))
        elif _is_url(image):
            images.append(image)
    listing['images'] = images
    listing['image'] = images[0] if images else None

    text_details = doc.get('text_details') or {}
    listing['title'] = text_details.get('name') or listing.get('property_type')
    return listing


NORMALIZERS = {
    'zap': _zap_listing,
    'quinto_andar': _quinto_andar_listing,
}


def _in_target_city(doc):
    city = str(((doc.get('address') or {}).get('city')) or '').strip().lower()
    return city == CITY.strip().lower()


def load_listings():
    listings = []
    for source, label, _ in SOURCES:
        collection = client[MONGO_DATABASE][source]
        for doc in collection.find({}):
            if _in_target_city(doc):
                listings.append(NORMALIZERS[source](doc))

    listings.sort(key=lambda item: (item.get('total') or 10 ** 9))
    return listings


def city_counts():
    counts = {}
    for source, _, _ in SOURCES:
        collection = client[MONGO_DATABASE][source]
        counts[source] = sum(1 for doc in collection.find({}, {'address': 1}) if _in_target_city(doc))
    return counts


# ------------------------------------------------------------------ crawls ---
def _seed_start_urls(redis_client, spider, data_file, key):
    for stale_key in (f'{spider}:dupefilter', f'{spider}:requests', key):
        redis_client.delete(stale_key)

    payload = json.loads((BASE_DIR / data_file).read_text(encoding='utf-8'))
    redis_client.rpush(key, json.dumps(payload))


def _items_saved(output):
    match = re.search(r"'mongodb/items_saved':\s*(\d+)", output)
    return int(match.group(1)) if match else 0


def _run_crawl():
    redis_client = redis_lib.Redis(host=REDIS_HOST, port=REDIS_PORT, db=0)
    results = {}

    try:
        for spider, label, data_file in SOURCES:
            CRAWL_STATE.update({
                'spider': spider,
                'message': f'Coletando novos anúncios no {label}…',
            })
            _seed_start_urls(redis_client, spider, data_file, f'{spider}:start_urls')

            process = subprocess.run(
                [
                    sys.executable, '-m', 'scrapy', 'crawl', spider,
                    '-s', 'LOG_LEVEL=INFO',
                    '-s', 'MAX_IDLE_TIME_BEFORE_CLOSE=25',
                    '-s', 'CLOSESPIDER_TIMEOUT=900',
                ],
                cwd=str(BASE_DIR),
                capture_output=True,
                text=True,
                encoding='utf-8',
                errors='replace',
                timeout=1200,
            )

            saved = _items_saved(process.stdout + process.stderr)
            results[spider] = {'saved': saved, 'exit_code': process.returncode}
            CRAWL_STATE['results'] = dict(results)

            if process.returncode != 0:
                raise RuntimeError(
                    f'{label}: o crawler terminou com código {process.returncode}. '
                    f'{(process.stderr or process.stdout).strip().splitlines()[-1:] or ""}'
                )

        CRAWL_STATE.update({
            'message': 'Atualização concluída',
            'finished_at': datetime.now().isoformat(timespec='seconds'),
        })
    except Exception as exc:  # noqa: BLE001 - surfaced to the UI
        CRAWL_STATE.update({
            'error': str(exc),
            'message': 'A busca falhou',
            'finished_at': datetime.now().isoformat(timespec='seconds'),
        })
    finally:
        CRAWL_STATE['running'] = False
        CRAWL_STATE['spider'] = None


def crawl_snapshot():
    snapshot = dict(CRAWL_STATE)
    snapshot['city'] = CITY
    snapshot['counts'] = city_counts()
    if snapshot.get('started_at') and snapshot.get('running'):
        try:
            snapshot['elapsed'] = int(
                (datetime.now() - datetime.fromisoformat(snapshot['started_at'])).total_seconds()
            )
        except ValueError:
            snapshot['elapsed'] = 0
    else:
        snapshot['elapsed'] = 0
    return snapshot


# ------------------------------------------------------------------- routes ---
@app.route('/')
def index():
    listings = load_listings()
    newest = max((item['scrapped_at'] for item in listings if item.get('scrapped_at')), default=None)
    return render_template(
        'index.html',
        listings=listings,
        sources=[{'id': source, 'label': label} for source, label, _ in SOURCES],
        city=CITY,
        city_center=CITY_CENTER,
        newest=newest,
    )


@app.route('/api/listings')
def api_listings():
    return jsonify(load_listings())


@app.route('/api/crawl', methods=['POST'])
def start_crawl():
    with CRAWL_LOCK:
        if CRAWL_STATE['running']:
            return jsonify({'started': False, 'error': 'Uma busca já está em andamento.'}), 409

        CRAWL_STATE.update({
            'running': True,
            'message': 'Iniciando busca…',
            'started_at': datetime.now().isoformat(timespec='seconds'),
            'finished_at': None,
            'results': {},
            'error': None,
        })
        threading.Thread(target=_run_crawl, name='rentcrawler-refresh', daemon=True).start()

    return jsonify({'started': True}), 202


@app.route('/api/crawl/status')
def crawl_status():
    return jsonify(crawl_snapshot())


ALLOWED_IMAGE_HOSTS = ('zapimoveis.com.br', 'vivareal.com.br', 'quintoandar.com.br')
IMAGE_REFERERS = {
    'zapimoveis': 'https://www.zapimoveis.com.br/',
    'vivareal': 'https://www.vivareal.com.br/',
    'quintoandar': 'https://www.quintoandar.com.br/',
}


@app.route('/img')
def image_proxy():
    url = request.args.get('u', '')
    parsed = urlparse(url)

    if parsed.scheme != 'https' or not parsed.hostname:
        abort(400)

    host = parsed.hostname.lower()
    if not any(host == allowed or host.endswith('.' + allowed) for allowed in ALLOWED_IMAGE_HOSTS):
        abort(403)

    referer = next((value for key, value in IMAGE_REFERERS.items() if key in host), None)
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 '
                      '(KHTML, like Gecko) Chrome/120.0 Safari/537.36',
        'Accept': 'image/avif,image/webp,image/apng,image/*,*/*;q=0.8',
    }
    if referer:
        headers['Referer'] = referer

    try:
        upstream = requests.get(url, headers=headers, timeout=20)
    except requests.RequestException:
        abort(502)

    if upstream.status_code != 200:
        abort(upstream.status_code)

    return Response(
        upstream.content,
        content_type=upstream.headers.get('Content-Type', 'image/jpeg'),
        headers={'Cache-Control': 'public, max-age=86400'},
    )


if __name__ == '__main__':
    app.run(host='127.0.0.1', port=8000, debug=os.environ.get('FLASK_DEBUG') == '1')
