from medusa.medusasystem import setRunningClusters, getRunningClusters


def test_running_cluster():
    """
    print running clusters
    """
    setRunningClusters()
    print getRunningClusters()

    # time.sleep(10)

if __name__ == "__main__":
    print "cluster working"
    test_running_cluster()
