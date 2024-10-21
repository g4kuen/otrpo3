import json
import os
import logging
import vk_api
from neo4j import GraphDatabase
from dotenv import load_dotenv
import argparse

load_dotenv()

logging.basicConfig(filename='app.log', filemode='w', format='%(name)s - %(levelname)s - %(message)s',
                    level=logging.INFO, encoding='utf-8')

parser = argparse.ArgumentParser(description="VK и Neo4j анализ")
parser.add_argument("--n", type=int, default=5, help="Количество пользователей или групп для выборки")
args = parser.parse_args()
n = args.n

def load_from_file(filename):
    if os.path.exists(filename):
        with open(filename, 'r') as file:
            return file.read().strip()
    else:
        raise FileNotFoundError(f"Файл {filename} не найден.")

def get_followers_at_depth(user_id, depth, collected_followers=None, collected_subscriptions=None, visited=None):
    if depth < 1:
        return collected_followers, collected_subscriptions

    if collected_followers is None:
        collected_followers = []
    if collected_subscriptions is None:
        collected_subscriptions = []
    if visited is None:
        visited = set()

    if user_id in visited:
        logging.warning(f"Пользователь {user_id} уже посещен, пропуск.")
        return collected_followers, collected_subscriptions

    visited.add(user_id)

    try:
        followers_response = vk.users.getFollowers(user_id=user_id, count=100)
        if not followers_response or 'items' not in followers_response:
            logging.error(f"Фолловеры не найдены для пользователя {user_id}.")
        else:
            followers = followers_response['items']
            logging.info(f"Найдено {len(followers)} фолловеров для пользователя {user_id}.")
            collected_followers.extend(followers)

        subscriptions_response = vk.users.getSubscriptions(user_id=user_id, extended=1)
        if not subscriptions_response or 'items' not in subscriptions_response:
            logging.error(f"Подписки не найдены для пользователя {user_id}.")
        else:
            subscriptions = subscriptions_response['items']
            logging.info(f"Найдено {len(subscriptions)} подписок для пользователя {user_id}.")
            collected_subscriptions.extend(subscriptions)
        if depth > 1:
            for follower in followers:
                get_followers_at_depth(follower, depth - 1, collected_followers, collected_subscriptions, visited)

    except vk_api.exceptions.ApiError as e:
        logging.error(f"Ошибка VK API для пользователя {user_id}: {str(e)}")

    return collected_followers, collected_subscriptions


class Neo4jHandler:
    def __init__(self, uri, user, password):
        self.driver = GraphDatabase.driver(uri, auth=(user, password))

    def close(self):
        self.driver.close()

    def create_user(self, user_id, screen_name, name, sex, home_town, about, photo_max):
        with self.driver.session() as session:
            session.write_transaction(self._create_user, user_id, screen_name, name, sex, home_town, about, photo_max)

    def create_group(self, group_id, name, screen_name):
        with self.driver.session() as session:
            session.write_transaction(self._create_group, group_id, name, screen_name)

    def create_follow_relation(self, follower_id, followee_id):
        with self.driver.session() as session:
            session.write_transaction(self._create_follow_relation, follower_id, followee_id)

    def create_subscribe_relation(self, subscriber_id, group_id):
        with self.driver.session() as session:
            session.write_transaction(self._create_subscribe_relation, subscriber_id, group_id)

    def get_total_users(self):
        with self.driver.session() as session:
            result = session.run("MATCH (u:User) RETURN COUNT(u) AS total_users")
            return result.single()['total_users']

    def get_total_groups(self):
        with self.driver.session() as session:
            result = session.run("MATCH (g:Group) RETURN COUNT(g) AS total_groups")
            return result.single()['total_groups']

    def get_top_users_by_followers(self, limit):
        with self.driver.session() as session:
            result = session.run("""
                MATCH (u:User)-[:FOLLOW]->(f:User)
                RETURN u.id AS user_id, COUNT(f) AS followers_count
                ORDER BY followers_count DESC
                LIMIT $limit
            """, limit=limit)
            return result.data()

    def get_top_groups_by_subscribers(self, limit):
        with self.driver.session() as session:
            result = session.run("""
                MATCH (g:Group)<-[:SUBSCRIBE]-(u:User)
                RETURN g.id AS group_id, COUNT(u) AS subscribers_count
                ORDER BY subscribers_count DESC
                LIMIT $limit
            """, limit=limit)
            return result.data()

    def get_mutual_followers(self):
        with self.driver.session() as session:
            result = session.run("""
                MATCH (u1:User)-[:FOLLOW]->(u2:User),
                      (u2)-[:FOLLOW]->(u1)
                RETURN u1.id AS user1_id, u2.id AS user2_id
            """)
            return result.data()

    @staticmethod
    def _create_user(tx, user_id, screen_name, name, sex, home_town, about, photo_max):
        tx.run(""" 
        MERGE (u:User {id: $user_id, screen_name: $screen_name, name: $name, sex: $sex, home_town: $home_town, about: $about, photo_max: $photo_max}) 
        """, user_id=user_id, screen_name=screen_name, name=name, sex=sex, home_town=home_town, about=about,
               photo_max=photo_max)

    @staticmethod
    def _create_group(tx, group_id, name, screen_name):
        tx.run("MERGE (g:Group {id: $group_id, name: $name, screen_name: $screen_name})",
               group_id=group_id, name=name, screen_name=screen_name)

    @staticmethod
    def _create_follow_relation(tx, follower_id, followee_id):
        tx.run(""" 
        MATCH (f:User {id: $follower_id}), (u:User {id: $followee_id}) 
        MERGE (f)-[:FOLLOW]->(u) 
        """, follower_id=follower_id, followee_id=followee_id)

    @staticmethod
    def _create_subscribe_relation(tx, subscriber_id, group_id):
        tx.run(""" 
        MATCH (u:User {id: $subscriber_id}), (g:Group {id: $group_id}) 
        MERGE (u)-[:SUBSCRIBE]->(g) 
        """, subscriber_id=subscriber_id, group_id=group_id)



access_token = os.getenv("VK_ACCESS_TOKEN")
user_id = os.getenv("VK_USER_ID")
neo4j_uri = os.getenv("NEO4J_URI")
neo4j_user = os.getenv("NEO4J_USER")
neo4j_password = os.getenv("NEO4J_PASSWORD")
depth = int(os.getenv("DEPTH", 2))

logging.info(f"Токен: {access_token}")
logging.info(f"ID пользователя: {user_id}")
logging.info(f"Глубина: {depth}")

vk_session = vk_api.VkApi(token=access_token)
vk = vk_session.get_api()
#
followers, subscriptions = get_followers_at_depth(user_id, depth)
logging.info(f"Фолловеры: {followers}")
logging.info(f"Подписки: {subscriptions}")

neo4j_handler = Neo4jHandler(uri=neo4j_uri, user=neo4j_user, password=neo4j_password)

for follower in followers:
    try:
        user_info = vk.users.get(user_id=follower, fields='first_name,last_name,sex,home_town,about,photo_max',
                                 name_case='nom')
        neo4j_handler.create_user(
            user_id=user_info[0]['id'],
            screen_name=user_info[0].get('screen_name', ''),
            name=f"{user_info[0]['first_name']} {user_info[0]['last_name']}",
            sex=user_info[0].get('sex'),
            home_town=user_info[0].get('home_town', '').title(),
            about=user_info[0].get('about', ''),
            photo_max=user_info[0].get('photo_max', '')
        )
        logging.info(f"Пользователь {follower} сохранен в Neo4j.")

        follower_subscriptions = vk.users.getSubscriptions(user_id=follower, extended=1)
        for group in follower_subscriptions.get('items', []):
            neo4j_handler.create_group(
                group_id=group['id'],
                name=group['name'],
                screen_name=group.get('screen_name', '')
            )
            neo4j_handler.create_subscribe_relation(user_info[0]['id'], group['id'])
            logging.info(f"Группа {group['id']} сохранена для пользователя {follower} в Neo4j.")

    except Exception as e:
        logging.error(f"Ошибка при сохранении пользователя {follower}: {e}")

for subscription in subscriptions:
    try:
        group_info = subscription
        neo4j_handler.create_group(
            group_id=group_info['id'],
            name=group_info['name'],
            screen_name=group_info.get('screen_name', '')
        )
        logging.info(f"Группа {group_info['id']} сохранена в Neo4j.")
    except Exception as e:
        logging.error(f"Ошибка при сохранении группы {group_info['id']}: {e}")


total_users = neo4j_handler.get_total_users()
total_groups = neo4j_handler.get_total_groups()
top_users = neo4j_handler.get_top_users_by_followers(n)
top_groups = neo4j_handler.get_top_groups_by_subscribers(n)
mutual_followers = neo4j_handler.get_mutual_followers()


print(f"Всего пользователей: {total_users}")
print(f"Всего групп: {total_groups}")
print(f"Топ {n} пользователей по количеству фолловеров:")
print(json.dumps(top_users, indent=4, ensure_ascii=False))
print(f"Топ {n} групп по количеству подписчиков:")
print(json.dumps(top_groups, indent=4, ensure_ascii=False))
print(f"Пользователи, которые являются фолловерами друг друга:")
print(json.dumps(mutual_followers, indent=4, ensure_ascii=False))


neo4j_handler.close()
