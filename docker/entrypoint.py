#!/usr/bin/env python2
import os
import sys
import commands

ENABLE_DATA_NODE = os.getenv('ENABLE_DATANODE') == 'true'
HTTP_PORT = os.getenv('CERESDB_HTTP_PORT', '5000')
GRPC_PORT = os.getenv('CERESDB_GRPC_PORT', '8831')
DATA_PATH = '/home/admin/data/ceresdb'

# hostname maybe return some ip(array)
def get_local_ip():
    return commands.getoutput('/usr/bin/localip').strip().split()[0]

def create_datanode_config():
    config = open('/etc/ceresdb/ceresdb.toml', 'r').read()
    config = config.replace("${HTTP_PORT}", HTTP_PORT)
    config = config.replace("${GRPC_PORT}", GRPC_PORT)
    config = config.replace("${NODE_ADDR}", get_local_ip())
    config = config.replace("${DATA_PATH}", DATA_PATH)
    open('/etc/ceresdb/ceresdb.toml', 'w').write(config)

def start_datanode():
    create_datanode_config()

    cmd = '''
# load env
. /ceresdb.env
env
exec /usr/bin/ceresdb-server --config /etc/ceresdb/ceresdb.toml
'''
    open('/usr/bin/ceresdb-start.sh', 'w').write(cmd)

def start_supervisord():
    port = int(os.getenv('SUPERVISORD_HTTP_PORT', '9001'))
    conf = '/etc/supervisor/supervisord.conf'
    if port:
        os.system(''' sed -i 's/:9001/:%d/g' %s ''' % (port, conf))
    open('/etc/supervisor/conf.d/touch-admin-cron.conf', 'a').write('\nkillasgroup=true\nstopasgroup=true\n')
    os.system('/usr/bin/supervisord -c %s --nodaemon' % conf)

def copy_environ():
    envs = []
    for k, v in os.environ.items():
        envs.append('export %s="%s"' % (k, v))
        # copy DATANODE_ to CSE_
        if 'DATANODE_' in k:
            envs.append('export %s="%s"' % (k.replace('DATANODE_', 'CSE_'), v))

    envs.append('export LOCAL_IP=%s' % get_local_ip())
    # support register ceres meta
    envs.append('export CSE_CERES_META_NODE_ADDR=%s' % (get_local_ip()))

    envs.append('export MALLOC_CONF=prof:true,prof_active:false,lg_prof_sample:19')

    open('/ceresdb.env', 'w').write('\n'.join(envs))

def init_dir():
    cmd = '''
mkdir -p /home/admin/logs /home/admin/data

# set logdir
mkdir -p /home/admin/logs/ceresdb

ln -nsf /data /home/admin/data

chmod +777 -R /data /home/admin/data /home/admin/logs
chown -R admin.admin /data /home/admin/data /home/admin/logs
'''
    open('/ceresdb-init.sh', 'w').write(cmd)
    os.system('sh /ceresdb-init.sh')

def main():
    print "copy_environ"
    copy_environ()

    print "init_dir"
    init_dir()

    if ENABLE_DATA_NODE:
        print "start_datanode"
        start_datanode()

    print "start_datanode"
    start_supervisord()

if __name__ == '__main__':
    main()
