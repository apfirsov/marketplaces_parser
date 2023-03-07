MAIN_MENU = ('https://static-basket-01.wb.ru/vol0/'
             'data/main-menu-ru-ru-v2.json')
BASE_URL = 'https://catalog.wb.ru/catalog/'
QUERY_PARAMS = '&appType=1&dest=-1029256,-102269,-1304596,-1281263'
CARD_URL = f'https://card.wb.ru/cards/detail?spp=30{QUERY_PARAMS}&nm='
LAST_PAGE_TRESHOLD = 95
MAX_PAGE = 100
MAX_ITEMS_IN_REQUEST = 750
MAX_ITEMS_IN_BRANDS_FILTER = 500
MAX_BRANDS_IN_REQUEST = 20
MIN_PRICE_RANGE = 20000
ATTEMPTS_COUNTER = 10
REQUEST_LIMIT = 200
WORKER_COUNT = 100
