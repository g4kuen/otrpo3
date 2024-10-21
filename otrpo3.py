import vk_api
import json
import os
#
with open('acess_token.txt', 'r') as file:
    access_token = file.read()
print(access_token)
vk_session = vk_api.VkApi(token=access_token)
vk = vk_session.get_api()

with open('vk_id.txt', 'r') as file:
    user_id = file.read().strip()

user_info = vk.users.get(user_ids=user_id, fields='followers_count')[0]

followers = vk.users.getFollowers(user_id=user_id, count=100)

subscriptions = vk.users.getSubscriptions(user_id=user_id, extended=1)

groups = vk.groups.get(user_id=user_id, extended=1)

data = {
    "user_info": user_info,
    "followers": followers,
    "subscriptions": subscriptions,
    "groups": groups
}

output_file = 'vk_data.json'
with open(output_file, 'w', encoding='utf-8') as f:
    json.dump(data, f, ensure_ascii=False, indent=4)

print(f'Данные сохранены в файл {output_file}')


