from data import YellowTaxi
from analyze import 
import argparse

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Description of your program')
    parser.add_argument('start_date', help='Enter date of first taxi pick-up')
    parser.add_argument('end_date', help='Enter date of last taxi pick-up')
    args = parser.parse_args()

    # Use the arguments as needed in your script
    print("Start Date:", args.start_date)
    print("End Date:", args.end_date)

    data_object=YellowTaxi(
        start_date=args.start_date,
        end_date=args.end_date
    )
    data_object.get_data()

