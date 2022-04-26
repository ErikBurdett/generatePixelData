import logging
import requests
import time
from concurrent.futures import ThreadPoolExecutor, as_completed

order_id = 1
order_amount = 10

test_adv1_body = 'https://listen.audiohook.com/08d79951-1908-4f0f-91aa-40eee5ac0e3f/pixel.png'
test_adv1_attr = f'https://listen.audiohook.com/08d79951-1908-4f0f-91aa-40eee5ac0e3f/pixel.png?order={order_id}&value={order_amount}'
test_adv2_body = 'https://listen.audiohook.com/08d79951-1908-4f0f-91aa-40eee5ac0e3f/pixel.png'
test_adv2_attr = f'https://listen.audiohook.com/08d79951-1908-4f0f-91aa-40eee5ac0e3f/pixel.png?order={order_id}&value={order_amount}'
test_adv3_body = 'https://listen.audiohook.com/08d79951-1908-4f0f-91aa-40eee5ac0e3f/pixel.png'
test_adv3_attr = f'https://listen.audiohook.com/08d79951-1908-4f0f-91aa-40eee5ac0e3f/pixel.png?order={order_id}&value={order_amount}'
test_adv4_body = 'https://listen.audiohook.com/08d79951-1908-4f0f-91aa-40eee5ac0e3f/pixel.png'
test_adv4_attr = f'https://listen.audiohook.com/08d79951-1908-4f0f-91aa-40eee5ac0e3f/pixel.png?order={order_id}&value={order_amount}'

THREAD_POOL = 16

session = requests.Session()
session.mount(
    'https://',
    requests.adapters.HTTPAdapter(pool_maxsize=THREAD_POOL,
                                  max_retries=3,
                                  pool_block=True)
)

def get(url):
    response = session.get(url)
    logging.info("request was completed in %s seconds [%s]", response.elapsed.total_seconds(), response.url)
    if response.status_code != 200:
        logging.error("request failed, error code %s [%s]", response.status_code, response.url)
    if 500 <= response.status_code < 600:
        # server is overloaded? give it a break
        time.sleep(5)
    return response

def download(urls):
    with ThreadPoolExecutor(max_workers=THREAD_POOL) as executor:
        # wrap in a list() to wait for all requests to complete
        for response in list(executor.map(get, urls)):
            if response.status_code == 200:
                print(response.content)

def main():
    logging.basicConfig(
        format='%(asctime)s.%(msecs)03d %(levelname)-8s %(message)s',
        level=logging.INFO,
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    

    urls = [
        test_adv1_body,
        test_adv1_attr,
        test_adv2_body,
        test_adv2_attr,
        test_adv3_body,
        test_adv3_attr,
        test_adv4_body,
        test_adv4_attr,
    ]

    download(urls)

if __name__ == "__main__":
    main()

