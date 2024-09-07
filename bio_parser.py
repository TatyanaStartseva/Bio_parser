import datetime
import io
import logging
import os
import sys
import asyncio
import random
import aiohttp
import re
import openpyxl
from dotenv import load_dotenv
from functools import wraps
from aiohttp import ClientTimeout, ClientProxyConnectionError
from aiogram import Bot
from db.db import db
from pymongo import ASCENDING
from datetime import datetime

logging.basicConfig(level=logging.INFO)
load_dotenv()
API_TOKEN = os.getenv("API_TOKEN")
bot = Bot(token=API_TOKEN)
HOST = os.getenv("HOST")
DATABASE = os.getenv("DATABASE")
USER = os.getenv("USERNAME_DB")
PASSWORD = os.getenv("PASSWORD_DB")
path_proxy = "proxy.txt"
users_ids = []


async def load_users_ids():
    global users_ids
    with open("users_ids.txt", "r") as f:
        users_ids = [int(line.strip()) for line in f.readlines()]
    return users_ids


def retry(retries):
    def decorator(func):
        @wraps(func)
        async def wrappper(*args, **kwargs):
            attempts = 0
            errors = 0
            while attempts < retries:
                try:
                    return await func(*args, **kwargs)
                except Exception as e:
                    errors += 1
                    attempts += 1
                    if errors == 3:
                        return e

        return wrappper

    return decorator


@retry(retries=3)
async def fetch(task_id, username, proxy):
    rand_proxy = random.choice(proxy)
    try:
        print(f"{task_id}: {username}")
        async with aiohttp.ClientSession() as session:
            url = f"https://t.me/{username}"
            async with session.get(url, proxy=rand_proxy) as response:
                if response.status == 200:
                    html_content = await response.text(encoding="utf-8")
                    match = re.search(
                        r'<meta property="og:description" content="([^"]*)"',
                        html_content,
                        re.IGNORECASE,
                    )
                    match_name = re.search(
                        r'<meta property="og:title" content="([^"]*)"', html_content
                    )
                    name = match_name.group(1) if match_name else None
                    if match and bool(match.group(1)):
                        bio = match.group(1)
                        if bio.startswith("You can contact"):
                            return {"name": name, "bio": None}
                        return {"name": name, "bio": bio}
                    else:
                        raise ValueError("Bio not found")
                else:
                    raise aiohttp.ClientResponseError(
                        status=response.status,
                        message=f"Failed to fetch {username}: Status {response.status}",
                    )
    except ClientProxyConnectionError as e:
        logging.error(
            f"Ошибка подключения к прокси-серверу: {e} \n Использовался прокси: {rand_proxy} \n Обрабатывался пользователь: {username}"
        )
        raise
    except Exception as e:
        logging.error(
            f"Ошибка в fetch {e} \n Использовался прокси: {rand_proxy} \n Обрабатывался пользователь: {username}"
        )
        raise


async def fetch_all(usernames, proxy):
    tasks = [
        asyncio.create_task(fetch(i, username["username"], proxy))
        for i, username in enumerate(usernames)
    ]
    responses = await asyncio.gather(*tasks, return_exceptions=True)
    return responses


async def process_users():
    proxy = []
    with open(path_proxy, "r") as f:
        for line in f:
            host, port, user, password = line.strip().split(":")
            proxy.append(f"http://{user}:{password}@{host}:{port}")
    try:
        users_ids = await load_users_ids()
        pool = db()
        cursor_users = pool["users"]
        size = 15000
        cursor = cursor_users.find({}).sort({"dateUpdated": 1}).limit(size)
        documents = await cursor.to_list(length=size)
        if documents:
            min_date = (
                documents[0]["dateUpdated"].replace(microsecond=0)
                if "dateUpdated" in documents[0]
                else datetime.now()
            )
            max_date = (
                documents[-1]["dateUpdated"].replace(microsecond=0)
                if "dateUpdated" in documents[-1]
                else datetime.now()
            )
        for user_id in users_ids:
            await bot.send_message(
                user_id,
                f"Начинаю обработку пользователей с {min_date} по {max_date}",
            )
        usernames = [
            {
                "username": record["username"],
                "bio": record["bio"],
                "user_id": record["user_id"],
            }
            for record in documents
        ]
        futures = []
        future = asyncio.create_task(fetch_all(usernames, proxy))
        futures.append(future)
        access_request = 0
        fail_request = 0
        responses = await asyncio.gather(*futures, return_exceptions=True)
        flattened_responses = [
            response for responses in responses for response in responses
        ]
        wb = openpyxl.Workbook()
        ws = wb.active
        ws.append(["username", "new bio", "old bio", "ban", "changed"])
        ban_values = []
        updates = []
        for username_dict, response in zip(usernames, flattened_responses):
            username = username_dict["username"]
            bio = username_dict["bio"]
            user_id = username_dict["user_id"]
            if (
                not isinstance(response, Exception)
                and response["bio"] != "Bio not found"
            ):
                first_name = last_name = None
                if response["name"]:
                    name_parts = response["name"].split()
                    first_name = name_parts[0]
                    if len(name_parts) == 2:
                        last_name = name_parts[1]
                if bio is not None and response["bio"] is not None:
                    if str(bio).lower().replace(" ", "") != str(
                        response["bio"]
                    ).lower().replace(" ", ""):
                        update_operation = cursor_users.update_one(
                            {"user_id": user_id},
                            {
                                "$set": {
                                    "username": username,
                                    "bio": response["bio"],
                                    "first_name": first_name,
                                    "last_name": last_name,
                                    "dateUpdated": datetime.now(),
                                }
                            },
                        )
                        updates.append(update_operation)
                        ws.append(
                            [
                                f"{username}",
                                f'{response["bio"]}',
                                f"{bio}",
                                "False",
                                "True",
                            ]
                        )
                    else:
                        update_operation = cursor_users.update_one(
                            {"user_id": user_id},
                            {
                                "$set": {
                                    "dateUpdated": datetime.now(),
                                }
                            },
                        )
                        updates.append(update_operation)
                        ws.append(
                            [
                                f"{username}",
                                f'{response["bio"]}',
                                f"{bio}",
                                "False",
                                "False",
                            ]
                        )
                elif bio == "Default-value-for-parser":
                    update_operation = cursor_users.update_one(
                        {"user_id": user_id},
                        {
                            "$set": {
                                "username": username,
                                "bio": response["bio"],
                                "first_name": first_name,
                                "last_name": last_name,
                                "dateUpdated": datetime.now(),
                            }
                        },
                    )
                    updates.append(update_operation)
                    ws.append(
                        [
                            f"{username}",
                            f'{response["bio"]}',
                            f"{bio}",
                            "False",
                            "True",
                        ]
                    )
                elif response["bio"] is None:
                    update_operation = cursor_users.update_one(
                        {"user_id": user_id},
                        {
                            "$set": {
                                "dateUpdated": datetime.now(),
                            }
                        },
                    )
                    updates.append(update_operation)
                    ws.append(
                        [
                            f"{username}",
                            f'{response["bio"]}',
                            f"{bio}",
                            "False",
                            "False",
                        ]
                    )
                else:
                    update_operation = cursor_users.update_one(
                        {"user_id": user_id},
                        {
                            "$set": {
                                "username": username,
                                "bio": response["bio"],
                                "first_name": first_name,
                                "last_name": last_name,
                                "dateUpdated": datetime.now(),
                            }
                        },
                    )
                    updates.append(update_operation)
                    ws.append(
                        [
                            f"{username}",
                            f'{response["bio"]}',
                            f"{bio}",
                            "False",
                            "True",
                        ]
                    )
                access_request += 1
            else:
                ban_values.append((user_id,))
                fail_request += 1
                ws.append([f"{username}", "None", f"{bio}", "True", "True"])
        if ban_values:
            await cursor_users.update_many(
                {"user_id": {"$in": [user_id for user_id, in ban_values]}},
                {
                    "$set": {
                        "ban": True,
                        "dateUpdated": datetime.now(),
                    }
                },
            )
        await asyncio.gather(*updates)
        wb.save("info_parse_bio.xlsx")
        with open("info_parse_bio.xlsx", "rb") as f:
            file_content = f.read()
            for user_id in users_ids:
                byte_file_content = io.BytesIO(file_content)
                byte_file_content.name = "info_parse_bio.xlsx"
                byte_file_content.seek(0)
                await bot.send_message(
                    user_id,
                    f"Обработано {size} пользователей. \n С {min_date} по "
                    f"{max_date} \n Успешные запросы: {access_request}, "
                    f"\n Заблокированные пользователи: {fail_request} ",
                )
                await bot.send_document(user_id, byte_file_content)
    except Exception as e:
        logging.error(f"Ошибка в process_users: {e}")
        users_ids = await load_users_ids()
        for user_id in users_ids:
            await bot.send_message(user_id, f"Ошибка в process_users: {e} ")


def uhandled_exception(exc_type, exc_value, exc_traceback):
    if issubclass(exc_type, KeyboardInterrupt):
        sys.__excepthook__(exc_type, exc_value, exc_traceback)
        return
    loop = asyncio.new_event_loop()

    async def handle_exception():
        users_ids = await load_users_ids()
        for user_id in users_ids:
            await bot.send_message(
                user_id, f"Непойманное исключение {exc_value} {exc_traceback}"
            )
        logging.error(f"Непойманное исключение {exc_value} {exc_traceback}")

    loop.run_until_complete(handle_exception())


sys.excepthook = uhandled_exception


def handle_async_exception(loop, context):
    msg = context.get("exception", context["message"])

    async def handle_exception():
        users_ids = await load_users_ids()
        for user_id in users_ids:
            await bot.send_message(user_id, f"Непойманное исключение {msg}")
        logging.error(f"Непойманное исключение: {msg}")

    loop.create_task(handle_exception())


if __name__ == "__main__":
    try:
        loop = asyncio.get_event_loop()
        loop.set_exception_handler(handle_async_exception)
        loop.run_until_complete(process_users())
        with open("info_parse_bio.xlsx", "w") as f:
            pass
        sys.exit(0)
    except Exception as e:
        logging.error(f"Ошибка : {e}")
