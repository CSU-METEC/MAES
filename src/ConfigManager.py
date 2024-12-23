from datetime import datetime

class ConfigManager():

    CONFIG_MANAGER_SINGLETON = None

    @classmethod
    def _getSingleton(cls):
        if cls.CONFIG_MANAGER_SINGLETON is None:
            cls._initializeSingleton(DEFAULT_CONFIG)
        return cls.CONFIG_MANAGER_SINGLETON

    @classmethod
    def _initializeSingleton(cls, config):
        cls.CONFIG_MANAGER_SINGLETON = ConfigManager(config)

    @classmethod
    def getConfigVar(cls, varName):
        return cls._getSingleton()._getConfigVar(varName)

    @classmethod
    def getConfigVarAsTimestamp(cls, varName, phase='start'):
        val1 = cls.getConfigVar(varName, phase)

    @classmethod
    def expandPhase(cls, phaseName, **kwargsForPhase):
        return cls._getSingleton()._expandPhase(phaseName, **kwargsForPhase)

    @classmethod
    def expandDynamicTemplate(cls, templateName, **kwargs):
        return cls._getSingleton()._expandDynamicTemplate(templateName, **kwargs)

    @classmethod
    def asDict(cls, **kwargs):
        return cls._getSingleton()._asDict(**kwargs)

    @classmethod
    def serialize(cls):
        return cls._getSingleton()._serialize()


    def __init__(self, config):
        self.defaultConfig = config
        self.phaseList = list(self.defaultConfig.get('phaseValues', {}).keys())
        self.reversedPhaseList = list(reversed(self.phaseList))
        self.configForPhases = {}

    def _getConfigVar(self, varName):
        cContext, _ = self._getExpansionContext(None)
        varVal = cContext.get(varName)
        return varVal

    def _getExpansionContext(self, phaseName):
        retContext = {}
        thisContext = {}
        for singlePhase in self.phaseList:
            if singlePhase == phaseName:
                thisContext = self.configForPhases.get(singlePhase, {})
                continue
            contextForSinglePhase = self.configForPhases.get(singlePhase, {})
            retContext = {**retContext, **contextForSinglePhase}

        return retContext, thisContext

    def _expandPhase(self, phaseName, **kwargsForPhase):
        prevPhasesContext, thisPhaseContext = self._getExpansionContext(phaseName)
        configForPhase = {**thisPhaseContext, **kwargsForPhase}
        valsToExpand = self.defaultConfig.get("phaseValues", {}).get(phaseName, {})
        valsToExpand= {**valsToExpand, **kwargsForPhase}

        lookupMap = {**prevPhasesContext, **configForPhase, **kwargsForPhase}
        for singleVal, singleValTemplate in valsToExpand.items():
            if isinstance(singleValTemplate, str):
                expandedVal = singleValTemplate.format_map(lookupMap)
            else:
                expandedVal = singleValTemplate
            configForPhase[singleVal] = expandedVal
            # add the newly expand var into lookMap so it is available for future expansions in the same context
            lookupMap[singleVal] = expandedVal

        if phaseName == 'start':
            if kwargsForPhase.get("scenarioTimestamp", None):
                scenarioTimestamp = kwargsForPhase['scenarioTimestamp']
            else:
                scenarioTimestampFormat = lookupMap.get('scenarioTimestampFormat', '')
                scenarioTimestamp = datetime.now().strftime(scenarioTimestampFormat)
            configForPhase = {**configForPhase, 'scenarioTimestamp': scenarioTimestamp}

        self.configForPhases[phaseName] = configForPhase
        pass

    def _expandDynamicTemplate(self, templateName, **kwargs):
        fullExpansionContext = self._asDict(**kwargs, includeDynamicTemplates=False)
        template = self.defaultConfig['dynamicTemplates'][templateName]
        expandedVal = template.format_map(fullExpansionContext)
        return expandedVal

    def _asDict(self, includeDynamicTemplates=True, **kwargs):
        cContext, _ = self._getExpansionContext(None)
        fullExpansionContext = {**cContext, **kwargs}
        if includeDynamicTemplates:
            fullExpansionContext = {**fullExpansionContext, **self.defaultConfig['dynamicTemplates']}

        return fullExpansionContext

    def _serialize(self):
        return self.defaultConfig





