import dmtest.thin_migrate.migrate_thin as migrate_thin
import dmtest.thin_migrate.unit as unit

def register(tests):
    migrate_thin.register(tests)
    unit.register(tests)
