import argparse
import json

from utils import NosqlChecker

parser = argparse.ArgumentParser()
parser.add_argument('--url', type=str)
args = parser.parse_args()


checker = NosqlChecker(url=args.url)
checker.get_services()

with open('result.json', 'w') as f:
    json.dump(checker.services, f)
