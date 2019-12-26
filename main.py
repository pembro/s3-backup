import logging
import argparse
from SsbConfig import SsbConfig

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--log', dest='loglevel', action='store',
                        default='WARN',
                        choices=['DEBUG','INFO','WARN','ERROR'])
    args = parser.parse_args()
    logging.basicConfig(level=args.loglevel)

    ssbconfig = SsbConfig()
    #print(ssbconfig.sync_folders[0].get_bucket_objects())
    #print(ssbconfig.sync_folders[0].get_local_objects())
    ssbconfig.sync_with_s3()


