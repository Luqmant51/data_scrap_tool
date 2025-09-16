import csv
import random
from typing import List, Dict

def load_alabama_zips(path: str, zip_column: str = "zip", state_column: str = "state") -> List[str]:
    zips = []
    with open(path, newline='', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        for row in reader:
            st = row.get(state_column, "").strip().upper()
            z = row.get(zip_column, "").strip()
            if st == "AL" and len(z) == 5 and z.isdigit():
                zips.append(z)
    return list(set(zips))

class ALZipGenerator:
    def __init__(self, zip_list: List[str]):
        self.zips = zip_list

    def random_zip(self) -> str:
        return random.choice(self.zips)

    def random_zips(self, n: int, distinct: bool = False) -> List[str]:
        if distinct:
            return random.sample(self.zips, min(n, len(self.zips)))
        else:
            return [random.choice(self.zips) for _ in range(n)]

if __name__ == "__main__":
    # assume you have a full US ZIP file, e.g. from SimpleMaps or Kaggle
    # that has state and zip columns
    zip_list = load_alabama_zips("us_zips.csv", zip_column="zip", state_column="state")
    gen = ALZipGenerator(zip_list)
    print("One random Alabama ZIP:", gen.random_zip())
    print("Five distinct Alabama ZIPs:", gen.random_zips(5, distinct=True))
