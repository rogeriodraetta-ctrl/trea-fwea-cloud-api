"""Executar diariamente em host independente do Render; saída 1 exige alerta."""
import os
import sys
import urllib.request
import urllib.error
import json
from urllib.parse import urlparse


def main():
    url = os.environ.get('TTS_HEARTBEAT_URL', '')
    token = os.environ.get('TFA_INFRA_TOKEN', '')
    if urlparse(url).scheme != 'https' or not token:
        print('HEARTBEAT_CONFIG_ERROR', file=sys.stderr)
        return 2
    try:
        req = urllib.request.Request(url, data=b'', method='POST',
                                     headers={'Authorization': 'Bearer ' + token})
        with urllib.request.urlopen(req, timeout=15) as response:
            result = json.load(response)
            if response.status != 200 or result.get('redis_write_verified') is not True:
                raise ValueError('invalid_response')
    except Exception:
        print('HEARTBEAT_FAILED: verificar API/Redis', file=sys.stderr)
        return 1
    print('HEARTBEAT_OK')
    return 0


if __name__ == '__main__':
    sys.exit(main())
