import argparse
import difflib

def get_args():
    parser = argparse.ArgumentParser(description='cmd args')
    parser.add_argument('--expected', '-e', type=str, help='expected queries result file')
    parser.add_argument('--actual', '-a', type=str, help='actual queries result file')
    args = vars(parser.parse_args())
    return args

def main():
    args = get_args()
   
    # Load queries results.
    f_expected_path = args['expected']
    f_actual_path = args['actual']

    f_expected = open(f_expected_path, "r")
    expecteds = f_expected.readlines()

    f_actual = open(f_actual_path, "r")
    actuals = f_actual.readlines()

    # Diff them.
    diffs = difflib.context_diff(expecteds, actuals)
    diff_num = 0
    for diff in diffs:
        diff_num += 1
        print(diff)

    f_expected.close()
    f_actual.close()

    # If diff exists, write the actual to expected, we can use `git diff` to inspect the detail diffs.
    if diff_num != 0:
        f = open(f_expected_path, "w")
        f.writelines(actuals)
        f.close()
        # Test failed, just panic
        print("Test failed...")
        assert(False)

    # Haha, test passed!
    print("Test passed...")

if __name__ == '__main__':
    main()
