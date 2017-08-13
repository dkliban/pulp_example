from pulpcore.plugin import PulpPluginAppConfig


class PulpExamplePluginAppConfig(PulpPluginAppConfig):
    name = 'pulp_example.app'
    label = 'pulp_example'
