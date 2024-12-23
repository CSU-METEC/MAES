class SDMCache():

    @classmethod
    def registerCache(cls):
        import SimDataManager as sdm
        sdm.SimDataManager.getSimDataManager().registerCache(cls)

    @classmethod
    def resetCache(cls):
        pass
