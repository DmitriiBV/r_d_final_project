import requests, json, os, sys
from datetime import date
import datetime
from hdfs import InsecureClient

file_path = f"{os.path.abspath(os.path.dirname(__file__))}/"
file_directory_path = os.path.dirname(os.path.dirname(file_path))
config_directory_path = "configurations"
config_path = "config.yaml"
config_abspath = os.path.join(file_directory_path, config_directory_path, config_path)
sys.path.append(file_path)
sys.path.append(config_abspath)


from config import Config


def authorization(cfg):
    url = f"{cfg['url']}{cfg['endpoint']}"
    headers = {'content-type': cfg['content_type']}
    data = {'username': cfg['username'], 'password': cfg['password']}
    r = requests.post(url, headers=headers, data=json.dumps(data), timeout=10)
    return r.json()[cfg['token']]


def get_data(cfg, token):
    url = f"{cfg['url']}{cfg['endpoint']}"
    headers = {"content-type": cfg['content_type'], "Authorization": f"{cfg['auth']} {token}"}
    data = {"date": cfg['date']}
    r = requests.get(url, headers=headers, data=json.dumps(data), timeout=10)
    return r.json()


def save_data_to_file(data, config):
    cfg_save = config.get_config('SAVE')
    save_dir = cfg_save['directory']
    save_date = datetime.datetime.now().date().__str__()
    save_file = cfg_save['file_name']
    save_zone = cfg_save['zone']
    save_project_name = cfg_save['project_name']
    directory_year = save_date.replace('-', '_')[0:4]
    directory_year_month = save_date.replace('-', '_')[0:7]
    file_name = f"{save_file}_{save_date.replace('-', '_')}.json"
    client = InsecureClient(**config.get_config('HDFS_WEB_CLIENT'))
    with client.write(os.path.join(save_zone, save_project_name, save_dir, directory_year, directory_year_month,
                                   file_name)) as json_file:
        json.dump(data, json_file)


def app_API():
    config = Config(config_abspath)
    cfg_auth = config.get_config('AUTH')
    cfg_api = config.get_config('API')
    token = authorization(cfg_auth)
    data = get_data(cfg_api, token)
    print(data)
    #save_data_to_file(data, config)


if __name__ == '__main__':
    app_API()
