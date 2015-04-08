class SEFactory:
    def __init__(self):
        pass

    def getSE(self, label, params={}):
        try:
            if label not in ['dropbox', 'grid', 'local']:
                raise AttributeError("Attribute 'label' error: Not found in list")

            if label == 'dropbox':
                from ddm.DropboxSEPlugin import DropboxSEPlugin
                se = DropboxSEPlugin(params)

            elif label == 'grid':
                from ddm.GridSEPlugin import GridSEPlugin
                se = GridSEPlugin(params)

            elif label == 'local':
                from ddm.LocalSEPlugin import LocalSEPlugin
                se = LocalSEPlugin(params)

            else:
                se = SEPlugin()
        except Exception:
            print Exception.message

        return se

class SEPlugin(object):
    def __init__(self, params=None):
        pass

    def get(self, src, dest, fsize, fsum):
        raise NotImplementedError("SEPlugin.get not implemented")

    def put(self, src, dest):
        raise NotImplementedError("SEPlugin.put not implemented")