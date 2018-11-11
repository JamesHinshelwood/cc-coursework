import sys
from typing import Tuple, List, Callable

VoidFn = Callable[[], None]


def unimplemented():
    raise NotImplementedError


menu_options: List[Tuple[str, VoidFn, List[Tuple[str, VoidFn]]]] = \
    [("Exit", lambda: sys.exit(0), []),
     ("Define a Kubernetes cluster", unimplemented, [
         ("Review the cluster definition", unimplemented)
     ]),
     ("Launch the cluster on AWS", unimplemented, [
         ("Validate the cluster", unimplemented),
         ("Deploy the Kubernetes web-dashboard", unimplemented),
         ("Access the Kubernetes web-dashboard", unimplemented)
     ]),
     ("View the cluster", unimplemented, [
         ("Get the admin password", unimplemented),
         ("Get the admin service account token", unimplemented)
     ]),
     ("Delete the cluster", unimplemented, [])]


def print_menu_options():
    for i, (option, _, suboptions) in enumerate(menu_options):
        print(str(i) + ": " + option)
        for j, (suboption, _) in enumerate(suboptions):
            print("\t" + str(i) + str(j + 1) + ": " + suboption)


def get_menu_selection(input: str) -> VoidFn:
    try:
        first = int(input[0])
        if len(input) > 1:
            second = int(input[1])
            return menu_options[first][2][second - 1][1]
        else:
            return menu_options[first][1]
    except (ValueError, IndexError):
        print("Invalid selection")
        return lambda: None


if __name__ == '__main__':
    print_menu_options()
    while True:
        get_menu_selection(input("Please enter your choice: "))()
