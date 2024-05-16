import dmtest.vdo.compress_tests as vdo_compress
import dmtest.vdo.creation_tests as vdo_creation
import dmtest.vdo.dedupe_tests as vdo_dedupe

def register(tests):
    vdo_creation.register(tests)
    vdo_dedupe.register(tests)
    vdo_compress.register(tests)
