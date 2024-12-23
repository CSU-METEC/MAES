import AppUtils as au
import SiteMain2 as sm
# import GraphMain4 as gm
import SummaryMain as sum

def preMain():
    config, _ = au.getConfig()
    sm.main(config)
    # if not config["disableGraph"]:
    #     gm.graphMain(config)
    # if not config["disableSummary"]:
    #     sum.main(config)
    pass

if __name__ == "__main__":
    preMain()