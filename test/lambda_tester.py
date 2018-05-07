import temperature_logger as tl
import data_generator as dn


def main():
    context = None
    file_name, event = dn.gen_test_temperature_data("2018/05/07 11:12:00", 30,
                                                     "ASCENDING")
    tl.lambda_handler(event, context)


if __name__ == '__main__':
    main()
