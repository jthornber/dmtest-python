import dmtest.thin.creation_tests as thin_creation
import dmtest.thin.deletion_tests as thin_deletion
import dmtest.thin.discard_tests as thin_discard
import dmtest.thin.snapshot_tests as thin_snapshot
import dmtest.thin.external_origin_tests as thin_external_origin

def register(tests):
    thin_creation.register(tests)
    thin_deletion.register(tests)
    thin_discard.register(tests)
    thin_snapshot.register(tests)
    thin_external_origin.register(tests)