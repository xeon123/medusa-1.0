
"""
    This is a command interface that a user can use to tamper digests in execution, kill a job or a task.
    This interface runs on the client side.
"""
from medusa.scheduler.predictionranking import reset_prediction, reset_penalization


def start_interface():
    menu = True
    while menu:
        print "Fault injection executioner:"
        print "Your options are:"
        print "-----------------------------"
        print "1) Save prediction template"
        print "2) Exit"
        print ""

        choice = input("Choose your option: ")
        choice = int(choice)

        if choice == 2:
            menu = False
            continue

        host = raw_input("Choose the cluster: ")
        if choice == 1:
            reset_prediction.apply_async(queue=host).get()
            reset_penalization.apply_async(queue=host).get()
        else:
            print("Wrong selection")


if __name__ == '__main__':
    start_interface()
