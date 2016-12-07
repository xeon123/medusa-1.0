import sys

from numpy import array, mean, ones, std, zeros


# Evaluate the linear regression

def feature_normalize(X):
    """
    Returns a normalized version of X where
    the mean value of each feature is 0 and the standard deviation
    is 1. This is often a good preprocessing step to do when
    working with learning algorithms.
    """
    mean_r = []
    std_r = []

    X_norm = X

    n_c = X.shape[1]
    for i in range(n_c):
        m = mean(X[:, i])
        s = std(X[:, i])
        mean_r.append(m)
        std_r.append(s)

        if s == 0.0:
            s = 1.0

        X_norm[:, i] = (X_norm[:, i] - m) / s

    return X_norm, mean_r, std_r


def compute_cost(X, y, theta):
    """
    Comput cost for linear regression
    """
    # Number of training samples
    m = y.size

    predictions = X.dot(theta)

    sqErrors = (predictions - y)

    J = (1.0 / (2 * m)) * sqErrors.T.dot(sqErrors)

    return J


def gradient_descent(X, y, theta, alpha, num_iters):
    """
    Performs gradient descent to learn theta
    by taking num_items gradient steps with learning
    rate alpha
    """
    m = y.size
    J_history = zeros(shape=(num_iters, 1))

    for i in range(num_iters):

        predictions = X.dot(theta)
        theta_size = theta.size

        for it in range(theta_size):
            temp = X[:, it]
            temp.shape = (m, 1)

            errors_x1 = (predictions - y) * temp

            theta[it][0] -= alpha * (1.0 / m) * errors_x1.sum()

        J_history[i, 0] = compute_cost(X, y, theta)

    return theta, J_history


def estimate(data, estimation):
    # Load the dataset
    # data = loadtxt('ex1data3.txt', delimiter=',')
    if len(data) == 0:
        return sys.maxint

    matrix = []
    for row in data:
        # in the prediction of the job, 1,2,4,6 is input data read, output data
        # producted, nr maps, and nr reduces
        temp = [row[idx] for idx in (0, 1, 3, 5)]
        matrix.append(temp)

    d = array(matrix)
    data = d
    X = data[:, :3]
    y = data[:, 3]

    # number of training samples
    m = y.size

    y.shape = (m, 1)

    # Scale features and set them to zero mean
    x, mean_r, std_r = feature_normalize(X)

    # Add a column of ones to X (interception data)
    it = ones(shape=(m, 4))
    it[:, 1:4] = x

    # Some gradient descent settings
    iterations = 100
    alpha = 0.01

    # Init Theta and Run Gradient Descent
    theta = zeros(shape=(4, 1))

    theta, J_history = gradient_descent(it, y, theta, alpha, iterations)

    # Predict price of a 1650 sq-ft 3 br house
    v1 = v2 = v3 = 0.0
    if std_r[0] > 0.0:
        v1 = ((estimation[0] - mean_r[0]) / std_r[0])
    if std_r[1] > 0.0:
        v2 = ((estimation[1] - mean_r[1]) / std_r[1])
    if std_r[2] > 0.0:
        v3 = ((estimation[2] - mean_r[2]) / std_r[2])

    A = [1.0, v1, v2, v3]

    y = array(A).dot(theta)
    # print 'Predicted value: %f' % (y)

    return y[0]
