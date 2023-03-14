def assert_raises(callback):
    failed = False
    try:
        callback()
    except Exception:
        failed = True

    assert failed
