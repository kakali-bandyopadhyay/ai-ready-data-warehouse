"""Search the synthetic note corpus for the nearest chunks."""
from __future__ import annotations

import argparse
import json

from pipeline.vector_search import search_similar


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("query", nargs="?", default="synthetic software company growth")
    parser.add_argument("-k", type=int, default=5)
    parser.add_argument("--provider", choices=("test", "openai"), default="test")
    args = parser.parse_args()
    results = search_similar(args.query, args.k, provider=args.provider)
    print(json.dumps(results, indent=2, default=str))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
