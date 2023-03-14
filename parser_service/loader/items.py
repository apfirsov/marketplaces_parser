import asyncio
import sys
import time
from asyncio import Queue, Semaphore, Task, create_task
import datetime

import pydantic
from aiohttp import ClientSession
from constants import (ATTEMPTS_COUNTER, BASE_URL, CARD_URL,
                       LAST_PAGE_TRESHOLD, MAX_BRANDS_IN_REQUEST,
                       MAX_ITEMS_IN_BRANDS_FILTER, MAX_ITEMS_IN_REQUEST,
                       MAX_PAGE, MIN_PRICE_RANGE, QUERY_PARAMS, REQUEST_LIMIT,
                       WORKER_COUNT)
from db.models import Category
from db.session import get_db
from logger_config import parser_logger as logger
from schemas import ArticleSchema
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.sql.selectable import Select
from db.models import (
    Color,
    Brand,
    Item,
    Article,
    ArticlesHistory,
    Size,
    HistorySizeRelation
)


items_cnt = 0
items_set = set()

# TODO: Синхронные логи (асинк или сократить количество логирования)
# TODO: Проверить алгоритмы фильтрации
# TODO: Замеры в декораторе
# TODO: Код стайл
# TODO: Добавить БД
# TODO: Найти/сократить синхронные вставки
# TODO: После выполнения пунктов выше замеры, подбор параметров


class ItemsParser:

    def __init__(self, client_session: ClientSession) -> None:
        self._session = client_session
        self._timestamp = datetime.datetime.now()
        self._request_semaphore = Semaphore(REQUEST_LIMIT)
        self._categories_queue = Queue()
        self._ids_queue = Queue()
        self._cards_queue = Queue()
        self._db_queue = Queue()
        self._queues = (
            self._categories_queue,
            self._ids_queue,
            self._cards_queue,
            self._db_queue,
        )
        self._req_counter = 0

    async def start(self, categories) -> None:
        for category in categories.scalars():
            category_as_dict = category.__dict__
            shard = category_as_dict.get('shard')
            if shard and 'blackhole' not in shard and 'preset' not in shard:
                self._categories_queue.put_nowait(category_as_dict)

        for _ in range(WORKER_COUNT):
            create_task(self._get_cards())
            create_task(self._collect_data())
            create_task(self._get_items_ids())
        create_task(self._write_to_db())

        create_task(self._write_to_db())
        await create_task(self._waiter())

    async def _waiter(self) -> None:
        while True:
            for queue in self._queues:
                await queue.join()

            if all([queue.empty() for queue in self._queues]):
                break

    async def _get_data(self, url: str) -> dict:
        async with self._request_semaphore:
            attempts_counter = ATTEMPTS_COUNTER

            while attempts_counter:
                try:
                    async with self._session.get(url, ssl=False) as response:
                        if response.ok:
                            self._req_counter += 1
                            return await response.json(content_type=None)

                        logger.info('Bad response status %d at: %s',
                                    response.status, url)

                except Exception as err:
                    logger.info('request error at: %s, %s', url, err)

                logger.info('request at: %s, %d tries left',
                            url, attempts_counter)
                attempts_counter -= 1
                await asyncio.sleep(ATTEMPTS_COUNTER-attempts_counter)
                if not attempts_counter:
                    logger.critical('attempts_counter lost at: %s', url)
                    sys.exit()

    async def _get_items_ids(self) -> None:
        while True:
            category = await self._categories_queue.get()

            category_id = category.get('id')
            shard = category.get('shard')
            query = category.get('query')
            price_filter_url = (f'{BASE_URL}{shard}/v4/'
                                f'filters?{query}{QUERY_PARAMS}')

            response = await self._get_data(price_filter_url)

            ctg_filters = response.get('data').get('filters')
            for ctg_filter in ctg_filters:
                if ctg_filter.get('key') == 'priceU':
                    ctg_max_price = ctg_filter.get('maxPriceU')
                    break

            await self._basic_parsing(
                category_id, shard, query, 0, ctg_max_price)

            self._categories_queue.task_done()

            logger.info('parsed %s %s', shard, query)

    async def _basic_parsing(self, category_id: int, shard: str, query: str,
                             min_pr: int, max_pr: int) -> None:
        logger.info('basic parsing for %s %s, price range: %s;%s',
                    shard, query, min_pr, max_pr)

        price_lmt = f'&priceU={min_pr};{max_pr}'
        base_url = (f'{BASE_URL}{shard}/catalog?'
                    f'{QUERY_PARAMS}&{query}{price_lmt}')
        last_page_url = base_url + '&page=' + str(MAX_PAGE)

        response = await self._get_data(last_page_url)

        response_data = response.get('data').get('products')
        last_page_is_full = len(response_data) > LAST_PAGE_TRESHOLD

        if last_page_is_full:
            rnd_avg = round((max_pr + min_pr) // 2 + 100, -4)
            if rnd_avg - min_pr >= MIN_PRICE_RANGE:
                await self._basic_parsing(
                    category_id, shard, query, min_pr, rnd_avg)
                await self._basic_parsing(
                    category_id, shard, query, rnd_avg, max_pr)
            else:
                await self._parse_by_brand(
                    category_id, shard, query, price_lmt)
        else:
            await self._get_items_ids_chunk(category_id, base_url)

    async def _parse_by_brand(self, category_id: int, shard: str, query: str,
                              price_lmt: str) -> list[int]:
        base_url = (f'{BASE_URL}{shard}/catalog?'
                    f'{query}{QUERY_PARAMS}{price_lmt}')
        brand_filter_url = (f'{BASE_URL}{shard}/v4/filters?filters='
                            f'fbrand&{query}{QUERY_PARAMS}{price_lmt}')

        response = await self._get_data(brand_filter_url)

        brand_filters = response.get('data').get('filters')[0].get('items')
        concatenated_ids_list = []
        concatenated_ids = ''
        cnt = 1

        for brand in brand_filters:
            brand_id = brand.get('id')
            if brand.get('count') > MAX_ITEMS_IN_BRANDS_FILTER:
                concatenated_ids_list.append(str(brand_id))
            elif cnt < MAX_BRANDS_IN_REQUEST:
                concatenated_ids += (';', '')[len(
                    concatenated_ids) == 0] + str(brand_id)
                cnt += 1
            else:
                concatenated_ids_list.append(concatenated_ids)
                concatenated_ids = str(brand_id)
                cnt = 1

        if concatenated_ids:
            concatenated_ids_list.append(concatenated_ids)

        for idx, string in enumerate(concatenated_ids_list, 1):
            request_url = base_url + '&fbrand=' + string
            await self._get_items_ids_chunk(category_id, request_url)

            logger.info(
                'brand parsing for %s, %s: %d / %d requests done',
                category_id, price_lmt, idx, len(concatenated_ids_list))

    async def _traverse_pages(self, base_url: str, sorting: str) -> set:
        traversed_ids = set()
        base_url = base_url + sorting
        page = 1
        while page <= MAX_PAGE:
            url = base_url + '&page=' + str(page)
            response = await self._get_data(url)
            response_data = response.get('data').get('products')

            if not len(response_data):
                return traversed_ids

            for item in response_data:
                traversed_ids.add(item.get('id'))
            page += 1
        if sorting == '&sort=popular':
            traversed_ids.update(
                await self._traverse_pages(base_url, '&sort=pricedown'))
            traversed_ids.update(
                await self._traverse_pages(base_url, '&sort=priceup'))
        return traversed_ids

    async def _get_items_ids_chunk(
            self, category_id: int, base_url: str) -> None:
        traversed_ids = await self._traverse_pages(base_url, '&sort=popular')

        concatenated_ids = ''
        cnt = 0
        for item_id in traversed_ids:
            if cnt < MAX_ITEMS_IN_REQUEST:
                concatenated_ids += (';', '')[len(
                    concatenated_ids) == 0] + str(item_id)
                cnt += 1
            else:
                self._ids_queue.put_nowait((category_id, concatenated_ids))
                concatenated_ids = str(item_id)
                cnt = 1
        self._ids_queue.put_nowait((category_id, concatenated_ids))

    async def _get_cards(self) -> None:
        while True:
            category_id, concatenated_ids = await self._ids_queue.get()

            url = CARD_URL + concatenated_ids
            response = await self._get_data(url)
            response_data = response.get('data').get('products')

            self._cards_queue.put_nowait((category_id, response_data))
            self._ids_queue.task_done()

    async def _collect_data(self) -> None:
        while True:
            category_id, cards = await self._cards_queue.get()

            for item in cards:
                card_object = {'colors': {}, 'sizes': {}}
                try:
                    article_data = ArticleSchema(**item).dict()
                except pydantic.ValidationError:
                    logger.critical('validation error at article %d',
                                    item.get('id'))
                    sys.exit()

                card_object['brands'] = {
                    'id': item.get('brandId'),
                    'name': item.get('brand')
                }

                card_object['items'] = {
                    'id': item.get('root'),
                    'category': category_id,
                    'brand': item.get('brandId')
                }

                card_object['articles'] = {
                    'id': item.get('id'),
                    'item': item.get('root'),
                    'name': item.get('name')
                }

                card_object['articles_history'] = {
                    'article': item.get('id'),
                    'timestamp': self._timestamp,
                    'price_full': item.get('priceU'),
                    'price_with_discount': item.get('salePriceU'),
                    'sale': item.get('sale'),
                    'rating': item.get('rating'),
                    'feedbacks': item.get('feedbacks'),
                }

                colors = article_data.get('colors')
                if colors:
                    card_object['articles'].update(
                            {'color': 999999} if len(colors) > 1 else
                            {'color': colors[0].get('id')})
                    for color in colors:
                        card_object['colors'].update(
                            {color.get('id'): color.get('name')})

                sum_count = 0
                hash_sizes = {}
                for size in item.get('sizes'):
                    size_count = 0
                    size_name = size.get('name')
                    for stock in size.get('stocks'):
                        item_count = stock.get('qty')
                        if item_count:
                            size_count += item_count
                    hash_sizes[size_name] = size_count
                    sum_count += size_count

                    card_object['sizes'].update({size_name: size_count})

                card_object['articles_history'].update(
                    {'sum_count': sum_count})

                self._db_queue.put_nowait(card_object)
            self._cards_queue.task_done()

            logger.info('collected data for %d: %s items',
                        category_id, len(cards))

    # TODO Make annotation for function (Сделать аннотацию функции)
    async def _check_and_write(self, entity: any, data: dict, s) -> None:
        item_in_db = await s.get(entity, data["id"])
        if item_in_db is None:
            s.add(entity(**data))

    async def _write_to_db(self) -> None:
        while True:
            card = await self._db_queue.get()

            global items_cnt
            items_cnt += 1
            global items_set
            items_set.add(card['articles']['id'])

            if items_cnt % 10000 == 0:
                logger.critical('ITEMS COUNT <<< %d >>>', items_cnt)

            db = get_db()
            session: AsyncSession = await anext(db)

            colors: list = card["colors"]
            sizes: list = card["sizes"]
            brands: dict = card["brands"]
            items: dict = card["items"]
            articles: dict = card["articles"]
            articles_history: dict = card["articles_history"]

            async with session.begin():
                try:
                    # TODO Убрать цикл после доработки от Саши
                    for color in colors:
                        await self._check_and_write(Color, color, session)

                    await self._check_and_write(Brand, brands, session)
                    await self._check_and_write(Item, items, session)
                    await self._check_and_write(Article, articles, session)

                    history_in_db = ArticlesHistory(**articles_history)
                    session.add(history_in_db)

                    # size in db
                    # TODO Доработать запись в БД Size
                    db_sizes = {}
                    for size in sizes:
                        res = await session.scalars(
                            select(Size).where(Size.name == size["name"]))
                        size_in_db = res.one_or_none()
                        if size_in_db is None:
                            size_in_db = Size(name=size["name"])
                            session.add(size_in_db)
                        db_sizes[size["name"]] = (size["count"], size_in_db)
                    await session.flush()

                    # history_size_relation in db
                    for count, size_in_db in db_sizes.values():
                        session.add(
                            HistorySizeRelation(
                                history=history_in_db.id,
                                size=size_in_db.id,
                                count=count
                            )
                        )

                except Exception as err:
                    await session.rollback()
                    logger.critical("!!!!!!Error write_to_db!!!! %s", err)
                    raise err
                else:
                    await session.commit()
            self._db_queue.task_done()


async def load_all_items() -> None:
    start = time.time()

    db = get_db()
    session: AsyncSession = await anext(db)

    async with session.begin():
        selectable = select(Category)
        # selectable: Select = select(Category).where(Category.id.in_([63010]))
        # selectable: Select = select(Category).where(Category.id.in_([130558]))

        categories = await session.execute(selectable)

    async with ClientSession() as client_session:
        parser = ItemsParser(client_session)

        await parser.start(categories)

    finish = time.time()
    impl_time = finish - start
    logger.critical('got %d items in %d seconds, %d requests, set length - %d',
                    items_cnt, impl_time, parser._req_counter, len(items_set))


# 130545 30930
# 130558 129905
# 8340 1273
# 130268 220
# 63010 181282
# 9411 23559
