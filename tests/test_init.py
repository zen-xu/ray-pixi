def test_public_exports():
    import ray_pixi

    assert hasattr(ray_pixi, "PixiPlugin")
    assert hasattr(ray_pixi, "pixi")
    assert isinstance(ray_pixi.__version__, str)


def test_no_import_side_effects():
    # Registration is done via the RAY_RUNTIME_ENV_PLUGINS env var on each node,
    # not as an import side effect, so neither helper is exposed.
    import ray_pixi

    assert not hasattr(ray_pixi, "register")
    assert not hasattr(ray_pixi, "ray")
