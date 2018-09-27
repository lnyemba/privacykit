import sys
SYS_ARGS={}
if len(sys.argv) > 1 :
    N = len(sys.argv)
    for i in range(1,N) :
        value = 1
        
        if sys.argv[i].startswith('--') :
            key = sys.argv[i].replace('-','')
            
            if i + 1 < N and not sys.argv[i+1].startswith('--') :
                value = sys.argv[i + 1].strip()
            SYS_ARGS[key] = value
            i += 2
        elif 'action' not in SYS_ARGS:
            SYS_ARGS['action'] = sys.argv[i].strip()
        
