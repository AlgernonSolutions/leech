from toll_booth.alg_obj.aws.aws_obj.dynamo_scanner import LeechScanner


def show_leech_stages():
    scanner = LeechScanner()
    leech_stages = scanner.scan()
    for stage_name, sid_values in leech_stages.items():
        print(f'{stage_name}: {len(sid_values)}')


if __name__ == '__main__':
    show_leech_stages()
