import sys

if __name__ == '__main__':

    args = sys.argv[1:]

    if len(args) != 2:
        print "202: Invalid command"
        sys.exit(1)

    scope, lfn = args

    # /<site_prefix>/<space_token>/rucio/<scope>/md5(<scope>:<lfn>)[0:2]/md5(<scope:lfn>)[2:4]/<lfn>
    try:
        # for python 2.6
        import hashlib
        hash = hashlib.md5()
    except:
        # for python 2.4
        import md5
        hash = md5.new()

    correctedscope = "/".join(scope.split('.'))
    hash.update('%s:%s' % (scope, lfn))
    hash_hex = hash.hexdigest()[:6]
    print 'rucio/%s/%s/%s/%s' % (correctedscope, hash_hex[0:2], hash_hex[2:4], lfn)