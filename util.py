def gen_cut(cut:dict):
    cuts = ''
    for key, val in cut.items():
        if cuts == '':
            cuts = cuts + ''.join(['{}={}'.format(key,val)])
        else:
            cuts = cuts + ''.join(['&{}={}'.format(key,val)])

    return cuts

def gen_msr_dd(msr:list):
    '''
    Receives a list of drilldowns or measures to include in the call and returns
    the generated string separated by comma
    '''
    msrs = ''
    for val in msr:
        if msrs == '':
            msrs = msrs + ''.join(['{}'.format(val)])
        else:
            msrs = msrs + ''.join([',{}'.format(val)])

    return msrs
