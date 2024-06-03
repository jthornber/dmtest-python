from dmtest.assertions import assert_raises
from dmtest.vdo.utils import standard_stack, standard_vdo
import dmtest.device_mapper.dev as dmdev
import dmtest.vdo.vdo_stack as vs
import dmtest.tvm as tvm
import dmtest.units as units
import dmtest.utils as utils


def t_create(fix):
    with standard_vdo(fix) as vdo:
        pass

def register(tests):
    tests.register_batch(
        "/vdo/creation",
        [
            ("create01", t_create),
        ],
    )
