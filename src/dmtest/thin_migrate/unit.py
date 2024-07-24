from dmtest.thin.utils import standard_stack, standard_pool
import dmtest.pool_stack as ps
import dmtest.process as process
import dmtest.units as units
import dmtest.utils as utils
import logging as log
import subprocess

#---------------------------------

def t_insufficient_buffer_size(fix):
    data_dev = fix.cfg["data_dev"]
    thin_size = min(units.gig(1), utils.dev_size(data_dev) // 2)

    with standard_pool(fix, block_size = 4096, zero = True) as src_pool:
        with ps.new_thin(src_pool, thin_size, 0) as src_thin:
            with ps.new_thin(src_pool, thin_size, 1) as dest_thin:
                with src_thin.pause():
                    src_pool.message(0, f"reserve_metadata_snap")

                try:
                    process.run(f"thin_migrate --source-dev {src_thin} --dest-dev {dest_thin} --buffer-size-meg 1")
                except subprocess.CalledProcessError:
                    pass
                except:
                    raise
                else:
                    raise Exception("command succeeded without error")

                src_pool.message(0, f"release_metadata_snap")


def t_input_none_thin_device(fix):
    data_dev = fix.cfg["data_dev"]
    try:
        process.run(f"thin_migrate --source-dev {data_dev} --dest-file migrate_dest")
    except subprocess.CalledProcessError:
        pass
    except:
        raise
    else:
        raise Exception("command succeeded without error")


def register(tests):
    tests.register_batch(
        "/thin_migrate/unit",
        [
            ("insufficient_buffer_size", t_insufficient_buffer_size),
            ("input_none_thin_device", t_input_none_thin_device),
        ],
    )
