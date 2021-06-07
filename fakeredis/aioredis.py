import distutils.version

import aioredis


if aioredis.__version__ >= distutils.version.StrictVersion('2.0.0a1'):
    from ._aioredis2 import *
else:
    from ._aioredis1 import *
