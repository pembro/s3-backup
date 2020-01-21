import logging
import argparse
from s3backup.S3BackupConfig import SsbConfig


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--log', dest='loglevel', action='store',
                        default='WARN',
                        choices=['DEBUG', 'INFO', 'WARN', 'ERROR'])
    args = parser.parse_args()
    logging.basicConfig(level=args.loglevel)

    ssbconfig = SsbConfig()
    ssbconfig.sync_with_s3()


if __name__ == '__main__':
    main()
