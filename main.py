from  datetime import datetime
import argparse
import json

from utils import NosqlChecker


def main(url):
    checker = NosqlChecker(url=url)
    checker.get_services()

    with open('result.json', 'w') as f:
        json.dump(checker.services, f)


if __name__ == '__main__':
    start = datetime.now()
    parser = argparse.ArgumentParser()
    parser.add_argument('--url', type=str)
    args = parser.parse_args()
    main(args.url)
    print(f'Total time spent (seconds): {(datetime.now() - start).seconds}')
