import argparse
import logging
from pathlib import Path

import AppUtils as au
import Anonymizer

logger = logging.getLogger(__name__)


def getParser():
    parser = argparse.ArgumentParser(
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
        description="Restore an anonymized MAES bundle to its original form using a key file.",
    )
    parser.add_argument(
        '--bundle', '-bun',
        required=True,
        metavar='ANON_ZIP',
        help="Anonymized bundle zip produced by --anonymize",
    )
    parser.add_argument(
        '--key', '-k',
        required=True,
        metavar='KEY_JSON',
        help="Key file written alongside the anonymized bundle (*.key.json)",
    )
    parser.add_argument(
        '--output', '-out',
        required=True,
        metavar='ZIP_PATH',
        help="Output path for the restored bundle zip",
    )
    return parser


def preMain():
    logging.basicConfig(level=logging.INFO, format=au.LOG_FORMAT)

    parser = getParser()
    args   = parser.parse_args()

    Anonymizer.deanonymizeBundle(
        Path(args.bundle),
        Path(args.key),
        Path(args.output),
    )


if __name__ == '__main__':
    preMain()
