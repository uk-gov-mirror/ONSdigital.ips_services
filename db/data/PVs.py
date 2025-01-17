type_pv = """if row['PURPOSE'] in (20,21,22):
    row['TYPE_PV'] = 1
else:
    row['TYPE_PV'] = 2"""

fage_pv = """if row['KIDAGE'] in (0, 1):
    row['FAGE_PV'] = 1
elif (row['KIDAGE'] >= 2) and (row['KIDAGE'] <= 15):
    row['FAGE_PV'] = 2
else:
    row['FAGE_PV'] = 6

if (row['AGE'] > 1) or math.isnan(row['AGE']):
    row['FAGE_PV'] = 6
elif (row['AGE'] < 2) and math.isnan(row['KIDAGE']):
    row['FAGE_PV'] = 2"""

discnt_f2_pv = """if row['PACKAGE'] in (1,2) and row['FLOW'] in (1,2,3,4,5,6,7,8):
    row['DISCNT_F2_PV'] = 0.85"""

discnt_package_cost_pv = """packagecost = None
if row['PACKAGE'] in (1 ,2):
    if packagecost != 999999:
        if not packagecost==None:
            row['DISCNT_PACKAGE_COST_PV'] = row['DISCNT_F1_PV'] * packagecost
    else:
        row['DISCNT_PACKAGE_COST_PV'] = packagecost
row['DISCNT_PACKAGE_COST_PV'] = round(row['DISCNT_PACKAGE_COST_PV'], 1)"""

discnt_f1_pv = """if row['FLOW'] in (1, 2, 3, 4, 5, 6, 7, 8):
    row['DISCNT_F1_PV'] = 0.85"""

fares_imp_eligible_pv = """if (((row['FAREKEY'] == '1') or (row['FAREKEY'] == '1.0')) or ((row['FARES_IMP_FLAG_PV']) == 1)) and ((row['MINS_FLAG_PV']) == 0):
    row['FARES_IMP_ELIGIBLE_PV'] = 1
else:
    row['FARES_IMP_ELIGIBLE_PV'] = 0"""

fares_imp_flag_pv = """if (row['DVFARE'] == 999999) or math.isnan(row['DVFARE']) or (row['DVFARE'] == 0):
    row['FARES_IMP_FLAG_PV'] = 1
else:
    row['FARES_IMP_FLAG_PV'] = 0"""

stay_purpose_grp_pv = """if row['PURPOSE'] in (20, 21, 22, 24, 25):
    row['STAY_PURPOSE_GRP_PV'] = 1
elif row['PURPOSE'] in (10, 15, 16):
    row['STAY_PURPOSE_GRP_PV'] = 2
elif row['PURPOSE'] == 40:
    row['STAY_PURPOSE_GRP_PV'] = 3
elif row['PURPOSE'] in (11, 12):
    row['STAY_PURPOSE_GRP_PV'] = 4
elif row['PURPOSE'] in (17, 18, 70, 71):
    row['STAY_PURPOSE_GRP_PV'] = 5
else:
    row['STAY_PURPOSE_GRP_PV'] = 6"""

StayImpCtryLevel4_pv = """if row['STAYIMPCTRYLEVEL2_PV'] >= 1 and row['STAYIMPCTRYLEVEL2_PV'] <= 8:
    row['STAYIMPCTRYLEVEL4_PV'] = 1
elif row['STAYIMPCTRYLEVEL2_PV'] >= 11 and row['STAYIMPCTRYLEVEL2_PV'] <= 15:
    row['STAYIMPCTRYLEVEL4_PV'] = 2
elif row['STAYIMPCTRYLEVEL2_PV'] >= 21 and row['STAYIMPCTRYLEVEL2_PV'] <= 32:
    row['STAYIMPCTRYLEVEL4_PV'] = 3
elif row['STAYIMPCTRYLEVEL2_PV'] >= 41 and row['STAYIMPCTRYLEVEL2_PV'] <= 46:
    row['STAYIMPCTRYLEVEL4_PV'] = 4
else:
    row['STAYIMPCTRYLEVEL4_PV'] = 5"""

StayImpCtryLevel3_pv = """if row['STAYIMPCTRYLEVEL2_PV'] in (1,2,4,6,8):
    row['STAYIMPCTRYLEVEL3_PV'] = 1
elif row['STAYIMPCTRYLEVEL2_PV'] in (3,5,7):
    row['STAYIMPCTRYLEVEL3_PV'] = 2
elif row['STAYIMPCTRYLEVEL2_PV'] in (11,12,13):
    row['STAYIMPCTRYLEVEL3_PV'] = 3
elif row['STAYIMPCTRYLEVEL2_PV'] in (14,15):
    row['STAYIMPCTRYLEVEL3_PV'] = 4
elif row['STAYIMPCTRYLEVEL2_PV'] in (21,22,23,24,25):
    row['STAYIMPCTRYLEVEL3_PV'] = 5
elif row['STAYIMPCTRYLEVEL2_PV'] in (31,32):
    row['STAYIMPCTRYLEVEL3_PV'] = 6
elif row['STAYIMPCTRYLEVEL2_PV'] in (41,42):
    row['STAYIMPCTRYLEVEL3_PV'] = 7
elif row['STAYIMPCTRYLEVEL2_PV'] in (43,44,45,46):
    row['STAYIMPCTRYLEVEL3_PV'] = 8
elif row['STAYIMPCTRYLEVEL2_PV'] in (51,52):
    row['STAYIMPCTRYLEVEL3_PV'] = 9
elif row['STAYIMPCTRYLEVEL2_PV'] == 91:
    row['STAYIMPCTRYLEVEL3_PV'] = 10
else:
    row['STAYIMPCTRYLEVEL3_PV'] = 99"""

mig_flag_pv = """if row['LOSKEY'] > 0:
    row['MIG_FLAG_PV'] = 1
else:
    row['MIG_FLAG_PV'] = 0"""

am_pm_night_pv = """if row['PORTROUTE'] == 811 and row['AM_PM_NIGHT'] == 2:
    row['AM_PM_NIGHT_PV'] = 1
elif row['PORTROUTE'] == 811 or row['PORTROUTE'] == 812:
    row['AM_PM_NIGHT_PV'] = row['AM_PM_NIGHT']
else:
    row['AM_PM_NIGHT_PV'] = 1"""

weekday_end_pv = """if dataset == 'survey':
    weekday = float('nan')
    from datetime import datetime

    day = int(row['INTDATE'][:2])
    month = int(row['INTDATE'][2:4])
    year = int(row['INTDATE'][4:8])

    d = datetime(year,month,day)

    dayweek = (d.isoweekday() + 1) % 7

    if (row['PORTROUTE'] == 811):
        if (dayweek >= 2 and dayweek <= 5):
            weekday = 1
        else:
            weekday = 2
    else:
        if (dayweek >= 2 and dayweek <= 6):
            weekday = 1
        else:
            weekday = 2

    if (row['PORTROUTE'] == 811):
        row['WEEKDAY_END_PV'] = weekday
    elif (row['PORTROUTE'] >= 600):
        row['WEEKDAY_END_PV'] = 1
    else:
        row['WEEKDAY_END_PV'] = weekday
else:
    if (row['PORTROUTE'] == 811):
        row['WEEKDAY_END_PV'] = row['WEEKDAY']
    elif (row['PORTROUTE'] >= 600):
        row['WEEKDAY_END_PV'] = 1
    else:
        row['WEEKDAY_END_PV'] = row['WEEKDAY']"""

unsamp_region_grp_pv = """dvpc = 0
row['ARRIVEDEPART'] = int(row['ARRIVEDEPART'])
if dataset == 'survey':
    if not math.isnan(row['DVPORTCODE']):
        dvpc = int(row['DVPORTCODE'] / 1000)
    if row['PORTROUTE'] < 300:
        if row['DVPORTCODE'] == 999999 or math.isnan(row['DVPORTCODE']):
            if row['FLOW'] in (1,3):
                row['REGION'] = row['RESIDENCE']
            elif row['FLOW'] in (2,4):
                row['REGION'] = row['COUNTRYVISIT']
            else:
                row['REGION'] = ''
        else:
            row['REGION'] = dvpc
        if row['REGION'] in (8, 20, 31, 40, 51, 56, 70, 100, 112, 191, 203, 208, 233, 234, 246, 250, 268, 276, 348, 352, 380, 398, 417, 428, 440, 442, 492, 498, 499, 528, 578, 616, 642, 643, 674, 688, 703, 705, 752, 756, 762, 795, 804, 807, 860, 940, 942, 943, 944, 945, 946, 950, 951):
            row['UNSAMP_REGION_GRP_PV'] = 1.0
        elif row['REGION'] in (124, 304, 630, 666, 840, 850):
            row['UNSAMP_REGION_GRP_PV'] = 2.0
        elif row['REGION'] in (4, 36, 50, 64, 96, 104, 116, 126, 144, 156, 158, 242, 356, 360, 408, 410, 418, 446, 458, 462, 496, 524, 554, 586, 608, 626, 702, 704, 764):
            row['UNSAMP_REGION_GRP_PV'] = 3.0
        elif row['REGION'] in (12, 24, 48, 72, 108, 120, 132, 140, 148, 174, 178, 180, 204, 226, 231, 232, 262, 266, 270, 288, 324, 348, 384, 404, 426, 430, 434, 450, 454, 466, 478, 480, 504, 508, 516, 562, 566, 624, 646, 654, 678, 686, 690, 694, 706, 710, 716, 732, 736, 748, 768, 788, 800, 818, 834, 854, 894):
            row['UNSAMP_REGION_GRP_PV'] = 4.0
        elif row['REGION'] == 392:
            row['UNSAMP_REGION_GRP_PV'] = 5.0
        elif row['REGION'] == 344:
            row['UNSAMP_REGION_GRP_PV'] = 6.0
        elif row['REGION'] in (16, 28, 32, 44, 48, 52, 60, 68, 76, 84, 90, 92, 136, 152, 166, 170, 184, 188, 192, 212, 214, 218, 222, 238, 254, 258, 296, 308, 312, 316, 320, 328, 332, 340, 364, 368, 376, 388, 400, 414, 422, 474, 484, 500, 512, 520, 530, 533, 540, 548, 558, 580, 581, 584, 591, 598, 604, 634, 638, 659, 660, 662, 670, 682, 690, 740, 760, 776, 780, 784, 796, 798, 858, 862, 882, 887, 949):
            row['UNSAMP_REGION_GRP_PV'] = 7.0
        elif row['REGION'] == 300:
            row['UNSAMP_REGION_GRP_PV'] = 8.0
        elif row['REGION'] in (292, 620, 621, 911, 912):
            row['UNSAMP_REGION_GRP_PV'] = 9.0
        elif row['REGION'] in (470, 792, 901, 902):
            row['UNSAMP_REGION_GRP_PV'] = 10.0
        elif row['REGION'] == 372:
            row['UNSAMP_REGION_GRP_PV'] = 11.0
        elif row['REGION'] in (831, 832, 833, 931):
            row['UNSAMP_REGION_GRP_PV'] = 12.0
        elif row['REGION'] in (921, 923, 924, 926, 933):
            row['UNSAMP_REGION_GRP_PV'] = 13.0
elif dataset == 'unsampled':
    if not math.isnan(row['REGION']):
        row['REGION'] = int(row['REGION'])
        row['UNSAMP_REGION_GRP_PV'] = row['REGION']

if row['UNSAMP_PORT_GRP_PV'] == 'A201' and row['UNSAMP_REGION_GRP_PV'] == 7.0 and row['ARRIVEDEPART'] == 2:
    row['UNSAMP_PORT_GRP_PV'] = 'A191'
if row['UNSAMP_PORT_GRP_PV'] == 'HGS':
    row['UNSAMP_PORT_GRP_PV'] = 'HBN'
if row['UNSAMP_PORT_GRP_PV'] == 'E921':
    row['UNSAMP_PORT_GRP_PV'] = 'E911'
if row['UNSAMP_PORT_GRP_PV'] == 'E951':
    row['UNSAMP_PORT_GRP_PV'] = 'E911'

if row['UNSAMP_PORT_GRP_PV'] == 'A181' and row['UNSAMP_REGION_GRP_PV'] == 6.0 and row['ARRIVEDEPART'] == 1:
    row['UNSAMP_PORT_GRP_PV'] = 'A151'
if row['UNSAMP_PORT_GRP_PV'] == 'A211' and row['UNSAMP_REGION_GRP_PV'] == 4.0 and row['ARRIVEDEPART'] == 1:
    row['UNSAMP_PORT_GRP_PV'] = 'A221'
if row['UNSAMP_PORT_GRP_PV'] == 'A241' and row['UNSAMP_REGION_GRP_PV'] == 8.0 and row['ARRIVEDEPART'] == 1:
    row['UNSAMP_PORT_GRP_PV'] = 'A201'

if row['UNSAMP_PORT_GRP_PV'] == 'RSS' and row['ARRIVEDEPART'] == 1:
    row['UNSAMP_PORT_GRP_PV'] = 'HBN'
if row['UNSAMP_PORT_GRP_PV'] == 'RSS' and row['ARRIVEDEPART'] == 2:
    row['UNSAMP_PORT_GRP_PV'] = 'HBN'"""

imbal_port_grp_pv = """if row['PORTROUTE'] in (111, 113, 119, 161, 171):
    row['IMBAL_PORT_GRP_PV'] = 1
elif row['PORTROUTE'] in (121, 123, 151, 153, 162, 165, 172, 175):
    row['IMBAL_PORT_GRP_PV'] = 2
elif row['PORTROUTE'] in (131, 132, 133, 134, 135, 163, 173):
    row['IMBAL_PORT_GRP_PV'] = 3
elif row['PORTROUTE'] in (141, 142, 143, 144, 145, 164, 174):
    row['IMBAL_PORT_GRP_PV'] = 4
elif row['PORTROUTE'] in (191, 193):
    row['IMBAL_PORT_GRP_PV'] = 5
elif row['PORTROUTE'] in (181, 183, 189, 199):
    row['IMBAL_PORT_GRP_PV'] = 6
elif row['PORTROUTE'] in (211, 213, 219, 221, 223, 231):
    row['IMBAL_PORT_GRP_PV'] = 7
elif row['PORTROUTE'] in (201, 203):
    row['IMBAL_PORT_GRP_PV'] = 8
elif row['PORTROUTE'] in (241, 243, 311, 319, 321, 351, 361, 371, 381, 391, 401, 411, 421, 441, 451, 461, 471, 481):
    row['IMBAL_PORT_GRP_PV'] = 9
elif row['PORTROUTE'] in (641, 671, 672, 681, 682, 691, 692, 731):
    row['IMBAL_PORT_GRP_PV'] = 10
elif row['PORTROUTE'] in (611, 612, 701, 711, 721, 722, 812):
    row['IMBAL_PORT_GRP_PV'] = 11
elif row['PORTROUTE'] in (621, 631, 632, 633, 634, 651, 661, 662):
    row['IMBAL_PORT_GRP_PV'] = 12
elif row['PORTROUTE'] == 911:
    row['IMBAL_PORT_GRP_PV'] = 13
elif row['PORTROUTE'] == 921:
    row['IMBAL_PORT_GRP_PV'] = 14
elif row['PORTROUTE'] == 811:
    row['IMBAL_PORT_GRP_PV'] = 15
elif row['IMBAL_PORT_GRP_PV'] == 9999:
    row['IMBAL_PORT_GRP_PV'] = 16
elif row['PORTROUTE'] == 951:
    row['IMBAL_PORT_GRP_PV'] = 17"""

imbal_eligible_pv = """if not math.isnan(row['FLOW']) and (row['RESPNSE'] > 0) and (row['RESPNSE'] < 3) and ((row['PURPOSE'] != 23) and (row['PURPOSE'] != 24) and (row['PURPOSE'] < 71 or math.isnan(row['PURPOSE']))) and (math.isnan(row['INTENDLOS']) or (row['INTENDLOS'] < 2) or (row['INTENDLOS'] > 7)):
    row['IMBAL_ELIGIBLE_PV'] = 1
else:
    row['IMBAL_ELIGIBLE_PV'] = 0"""

mins_flag_pv = """if row['TYPEINTERVIEW'] == 1:
    row['MINS_FLAG_PV'] = 2
elif row['RESPNSE'] == 1 or row['RESPNSE'] == 2:
    row['MINS_FLAG_PV'] = 0
elif row['RESPNSE'] == 3:
    row['MINS_FLAG_PV'] = 1
else:
    row['MINS_FLAG_PV'] = 3"""

nr_port_grp_pv = """row['NR_PORT_GRP_PV'] = row['PORTROUTE']"""

nr_flag_pv = """if row['RESPNSE'] > 0 and row['RESPNSE'] < 4:
    row['NR_FLAG_PV'] = 0
elif row['RESPNSE'] >= 4 and row['RESPNSE'] < 7:
    row['NR_FLAG_PV'] = 1
else:
    row['NR_FLAG_PV'] = 2"""

shift_port_grp_pv = """if row['PORTROUTE'] >= 161 and row['PORTROUTE'] <= 165:
    row['SHIFT_PORT_GRP_PV'] = 'LHR Transits'
elif row['PORTROUTE'] >= 171 and row['PORTROUTE'] <= 175:
    row['SHIFT_PORT_GRP_PV'] = 'LHR Mig Transits'
else:
    #  row['SHIFT_PORT_GRP_PV'] = str(row['PORTROUTE']).rjust(3,' ')
    row['SHIFT_PORT_GRP_PV'] = str(int(row['PORTROUTE']))"""

crossings_flag_pv = """if row['PORTROUTE'] < 600 or row['PORTROUTE'] > 900:
    row['CROSSINGS_FLAG_PV'] = 0
else:
    row['CROSSINGS_FLAG_PV'] = 1"""

shift_flag_pv = """if row['PORTROUTE'] < 600 or row['PORTROUTE'] > 900:
    row['SHIFT_FLAG_PV'] = 1
else:
    row['SHIFT_FLAG_PV'] = 0"""

unsamp_port_grp_pv = """if row['PORTROUTE'] in (111, 113, 119, 161):
    row['UNSAMP_PORT_GRP_PV'] = 'A111'
elif row['PORTROUTE'] in (121, 123, 162, 172):
    row['UNSAMP_PORT_GRP_PV'] = 'A121'
elif row['PORTROUTE'] in (131, 132, 133, 134, 135, 163, 173):
    row['UNSAMP_PORT_GRP_PV'] = 'A131'
elif row['PORTROUTE'] in (141, 142, 143, 144, 145, 164):
    row['UNSAMP_PORT_GRP_PV'] = 'A141'
elif row['PORTROUTE'] in (151, 152, 153, 165):
    row['UNSAMP_PORT_GRP_PV'] = 'A151'
elif row['PORTROUTE'] in (181, 183, 189):
    row['UNSAMP_PORT_GRP_PV'] = 'A181'
elif row['PORTROUTE'] in (191, 192, 193, 199):
    row['UNSAMP_PORT_GRP_PV'] = 'A191'
elif row['PORTROUTE'] in (201, 202, 203):
    row['UNSAMP_PORT_GRP_PV'] = 'A201'
elif row['PORTROUTE'] in (211, 213, 219):
    row['UNSAMP_PORT_GRP_PV'] = 'A211'
elif row['PORTROUTE'] in (221, 223):
    row['UNSAMP_PORT_GRP_PV'] = 'A221'
elif row['PORTROUTE'] in (231, 232):
    row['UNSAMP_PORT_GRP_PV'] = 'A231'
elif row['PORTROUTE'] in (241, 243):
    row['UNSAMP_PORT_GRP_PV'] = 'A241'
elif row['PORTROUTE'] in (381, 382, 391, 341, 331, 451):
    row['UNSAMP_PORT_GRP_PV'] = 'A991'
elif row['PORTROUTE'] in (401, 411, 441, 471):
    row['UNSAMP_PORT_GRP_PV'] = 'A992'
elif row['PORTROUTE'] in (311, 371, 421, 321, 319):
    row['UNSAMP_PORT_GRP_PV'] = 'A993'
elif row['PORTROUTE'] in (461, 351, 361, 481):
    row['UNSAMP_PORT_GRP_PV'] = 'A994'
elif row['PORTROUTE'] == 991:
    row['UNSAMP_PORT_GRP_PV'] = 'A991'
elif row['PORTROUTE'] == 992:
    row['UNSAMP_PORT_GRP_PV'] = 'A992'
elif row['PORTROUTE'] == 993:
    row['UNSAMP_PORT_GRP_PV'] = 'A993'
elif row['PORTROUTE'] == 994:
    row['UNSAMP_PORT_GRP_PV'] = 'A994'
elif row['PORTROUTE'] == 995:
    row['UNSAMP_PORT_GRP_PV'] = 'ARE'
elif row['PORTROUTE'] in (611, 612, 613, 851, 853, 868, 852):
    row['UNSAMP_PORT_GRP_PV'] = 'DCF'
elif row['PORTROUTE'] in (621, 631, 632, 633, 634, 854):
    row['UNSAMP_PORT_GRP_PV'] = 'SCF'
elif row['PORTROUTE'] in (641, 865):
    row['UNSAMP_PORT_GRP_PV'] = 'LHS'
elif row['PORTROUTE'] in (635, 636, 651, 652, 661, 662, 856):
    row['UNSAMP_PORT_GRP_PV'] = 'SLR'
elif row['PORTROUTE'] in (671, 859, 860, 855):
    row['UNSAMP_PORT_GRP_PV'] = 'HBN'
elif row['PORTROUTE'] in (672, 858):
    row['UNSAMP_PORT_GRP_PV'] = 'HGS'
elif row['PORTROUTE'] in (681, 682, 691, 692, 862):
    row['UNSAMP_PORT_GRP_PV'] = 'EGS'
elif row['PORTROUTE'] in (701, 711, 741, 864):
    row['UNSAMP_PORT_GRP_PV'] = 'SSE'
elif row['PORTROUTE'] in (721, 722, 863):
    row['UNSAMP_PORT_GRP_PV'] = 'SNE'
elif row['PORTROUTE'] in (731, 861):
    row['UNSAMP_PORT_GRP_PV'] = 'RSS'
elif row['PORTROUTE'] == 811:
    row['UNSAMP_PORT_GRP_PV'] = 'T811'
elif row['PORTROUTE'] == 812:
    row['UNSAMP_PORT_GRP_PV'] = 'T812'
elif row['PORTROUTE'] == 911:
    row['UNSAMP_PORT_GRP_PV'] = 'E911'
elif row['PORTROUTE'] == 921:
    row['UNSAMP_PORT_GRP_PV'] = 'E921'
elif row['PORTROUTE'] == 951:
    row['UNSAMP_PORT_GRP_PV'] = 'E951'

Irish = 0
IoM = 0
ChannelI = 0
dvpc = 0

if dataset == 'survey':
    if not math.isnan(row['DVPORTCODE']):
        dvpc = int(row['DVPORTCODE'] / 1000)

    if dvpc == 372:
        Irish = 1
    elif (row['DVPORTCODE'] == 999999) or math.isnan(row['DVPORTCODE']):
        if ((row['FLOW'] in (1,3)) and (row['RESIDENCE'] == 372)):
            Irish = 1
        elif ((row['FLOW'] in (2,4)) and (row['COUNTRYVISIT'] == 372)):
            Irish = 1

    if dvpc == 833:
        IoM = 1
    elif (row['DVPORTCODE'] == 999999) or math.isnan(row['DVPORTCODE']):
        if ((row['FLOW'] in (1,3)) and (row['RESIDENCE'] == 833)):
            IoM = 1
        elif ((row['FLOW'] in (2,4)) and (row['COUNTRYVISIT'] == 833)):
            IoM = 1

    if dvpc in (831, 832, 931):
        ChannelI = 1

    elif (row['DVPORTCODE'] == 999999) or math.isnan(row['DVPORTCODE']):
        if ((row['FLOW'] in (1,3)) and (row['RESIDENCE'] in (831, 832, 931))):
            ChannelI = 1
        elif ((row['FLOW'] in (2,4)) and (row['COUNTRYVISIT'] in (831, 832, 931))):
            ChannelI = 1

    if (Irish) and row['PORTROUTE'] in (111, 121, 131, 141, 132, 142, 119, 161, 162, 163, 164, 165, 151, 152):
        row['UNSAMP_PORT_GRP_PV'] = 'AHE'
    elif (Irish) and row['PORTROUTE'] in (181, 191, 192, 189, 199):
        row['UNSAMP_PORT_GRP_PV'] = 'AGE'
    elif (Irish) and row['PORTROUTE'] in (211, 221, 231, 219, 249):
        row['UNSAMP_PORT_GRP_PV'] = 'AME'
    elif (Irish) and row['PORTROUTE'] == 241:
        row['UNSAMP_PORT_GRP_PV'] = 'ALE'
    elif (Irish) and row['PORTROUTE'] in (201, 202):
        row['UNSAMP_PORT_GRP_PV'] = 'ASE'
    elif (Irish) and (row['PORTROUTE'] >= 300) and (row['PORTROUTE'] < 600):
        row['UNSAMP_PORT_GRP_PV'] = 'ARE'
    elif (ChannelI) and (row['PORTROUTE'] >= 100) and (row['PORTROUTE'] < 300):
        row['UNSAMP_PORT_GRP_PV'] = 'MAC'
    elif (ChannelI) and (row['PORTROUTE'] >= 300) and (row['PORTROUTE'] < 600):
        row['UNSAMP_PORT_GRP_PV'] = 'RAC'
    elif (IoM) and (row['PORTROUTE'] >= 100) and (row['PORTROUTE'] < 300):
        row['UNSAMP_PORT_GRP_PV'] = 'MAM'
    elif (IoM) and (row['PORTROUTE'] >= 300) and (row['PORTROUTE'] < 600):
        row['UNSAMP_PORT_GRP_PV'] = 'RAM'
"""

samp_port_grp_pv = """if row['PORTROUTE'] in (111, 113, 119, 161, 171):
    row['SAMP_PORT_GRP_PV'] = 'A111'
elif row['PORTROUTE'] in (121, 123, 162, 172):
    row['SAMP_PORT_GRP_PV'] = 'A121'
elif row['PORTROUTE'] in (131, 132, 133, 134, 135, 163, 173):
    row['SAMP_PORT_GRP_PV'] = 'A131'
elif row['PORTROUTE'] in (141, 142, 143, 144, 145, 164, 174):
    row['SAMP_PORT_GRP_PV'] = 'A141'
elif row['PORTROUTE'] in (151, 152, 153, 154, 165, 175):
    row['SAMP_PORT_GRP_PV'] = 'A151'
elif row['PORTROUTE'] in (181, 183, 189):
    row['SAMP_PORT_GRP_PV'] = 'A181'
elif row['PORTROUTE'] in (191, 192, 193, 199):
    row['SAMP_PORT_GRP_PV'] = 'A191'
elif row['PORTROUTE'] in (201, 202, 203, 204):
    row['SAMP_PORT_GRP_PV'] = 'A201'
elif row['PORTROUTE'] in (211, 213, 219):
    row['SAMP_PORT_GRP_PV'] = 'A211'
elif row['PORTROUTE'] in (221, 223):
    row['SAMP_PORT_GRP_PV'] = 'A221'
elif row['PORTROUTE'] in (231, 232, 234):
    row['SAMP_PORT_GRP_PV'] = 'A231'
elif row['PORTROUTE'] in (241, 243, 249):
    row['SAMP_PORT_GRP_PV'] = 'A241'
elif row['PORTROUTE'] in (311, 313, 319):
    row['SAMP_PORT_GRP_PV'] = 'A311'
elif row['PORTROUTE'] == 321:
    row['SAMP_PORT_GRP_PV'] = 'A321'
elif row['PORTROUTE'] == 331:
    row['SAMP_PORT_GRP_PV'] = 'A331'
elif row['PORTROUTE'] == 351:
    row['SAMP_PORT_GRP_PV'] = 'A351'
elif row['PORTROUTE'] == 361:
    row['SAMP_PORT_GRP_PV'] = 'A361'
elif row['PORTROUTE'] == 371:
    row['SAMP_PORT_GRP_PV'] = 'A371'
elif row['PORTROUTE'] in (381, 382):
    row['SAMP_PORT_GRP_PV'] = 'A381'
elif row['PORTROUTE'] in (341, 391, 393):
    row['SAMP_PORT_GRP_PV'] = 'A391'
elif row['PORTROUTE'] == 401:
    row['SAMP_PORT_GRP_PV'] = 'A401'
elif row['PORTROUTE'] == 411:
    row['SAMP_PORT_GRP_PV'] = 'A411'
elif row['PORTROUTE'] in (421, 423):
    row['SAMP_PORT_GRP_PV'] = 'A421'
elif row['PORTROUTE'] in (441, 443):
    row['SAMP_PORT_GRP_PV'] = 'A441'
elif row['PORTROUTE'] == 451:
    row['SAMP_PORT_GRP_PV'] = 'A451'
elif row['PORTROUTE'] == 461:
    row['SAMP_PORT_GRP_PV'] = 'A461'
elif row['PORTROUTE'] == 471:
    row['SAMP_PORT_GRP_PV'] = 'A471'
elif row['PORTROUTE'] == 481:
    row['SAMP_PORT_GRP_PV'] = 'A481'
elif row['PORTROUTE'] in (611, 612, 613):
    row['SAMP_PORT_GRP_PV'] = 'DCF'
elif row['PORTROUTE'] in (621, 631, 632, 633, 634, 651, 652, 662):
    row['SAMP_PORT_GRP_PV'] = 'SCF'
elif row['PORTROUTE'] == 641:
    row['SAMP_PORT_GRP_PV'] = 'LHS'
elif row['PORTROUTE'] in (635,636,661):
    row['SAMP_PORT_GRP_PV'] = 'SLR'
elif row['PORTROUTE'] == 671:
    row['SAMP_PORT_GRP_PV'] = 'HBN'
elif row['PORTROUTE'] == 672:
    row['SAMP_PORT_GRP_PV'] = 'HGS'
elif row['PORTROUTE'] == 681:
    row['SAMP_PORT_GRP_PV'] = 'EGS'
elif row['PORTROUTE'] in (701, 711, 741):
    row['SAMP_PORT_GRP_PV'] = 'SSE'
elif row['PORTROUTE'] in (721, 722):
    row['SAMP_PORT_GRP_PV'] = 'SNE'
elif row['PORTROUTE'] in (731, 682, 691, 692):
    row['SAMP_PORT_GRP_PV'] = 'RSS'
elif row['PORTROUTE'] in (811, 813):
    row['SAMP_PORT_GRP_PV'] = 'T811'
elif row['PORTROUTE'] == 812:
    row['SAMP_PORT_GRP_PV'] = 'T811'
elif row['PORTROUTE'] in (911, 913):
    row['SAMP_PORT_GRP_PV'] = 'E911'
elif row['PORTROUTE'] == 921:
    row['SAMP_PORT_GRP_PV'] = 'E921'
elif row['PORTROUTE'] == 951:
    row['SAMP_PORT_GRP_PV'] = 'E951'

Irish = 0
IoM = 0
ChannelI = 0
dvpc = 0

if dataset == 'survey':
    if not math.isnan(row['DVPORTCODE']):
        dvpc = int(row['DVPORTCODE'] / 1000)

    if dvpc == 372:
        Irish = 1
    elif row['DVPORTCODE'] == 999999 or math.isnan(row['DVPORTCODE']):
        if ((row['FLOW'] in (1,3)) and (row['RESIDENCE'] == 372)):
            Irish = 1
        elif ((row['FLOW'] in (2,4)) and (row['COUNTRYVISIT'] == 372)):
            Irish = 1

    if dvpc == 833:
        IoM = 1
    elif row['DVPORTCODE'] == 999999 or math.isnan(row['DVPORTCODE']):
        if ((row['FLOW'] in (1,3)) and (row['RESIDENCE'] == 833)):
            IoM = 1
        elif ((row['FLOW'] in (2,4)) and (row['COUNTRYVISIT'] == 833)):
            IoM = 1

    if dvpc in (831, 832, 931):
        ChannelI = 1
    elif row['DVPORTCODE'] == 999999 or math.isnan(row['DVPORTCODE']):
        if ((row['FLOW'] in (1,3)) and (row['RESIDENCE'] in (831, 832, 931))):
            ChannelI = 1
        elif ((row['FLOW'] in (2,4)) and (row['COUNTRYVISIT'] in (831, 832, 931))):
            ChannelI = 1
elif dataset == 'traffic':
    if row['HAUL'] == 'E':
        Irish = 1
    elif ( row['PORTROUTE'] == 250) or ( row['PORTROUTE'] == 350):
        ChannelI = 1
    elif ( row['PORTROUTE'] == 260) or (row['PORTROUTE'] == 360):
        IoM = 1

if (Irish) and row['PORTROUTE'] in (111, 121, 131, 141, 132, 142, 119, 161, 162, 163, 164, 165, 151, 152, 171, 173, 174, 175):
    row['SAMP_PORT_GRP_PV'] = 'AHE'
elif (Irish) and row['PORTROUTE'] in (181, 191, 192, 189, 199):
    row['SAMP_PORT_GRP_PV'] = 'AGE'
elif (Irish) and row['PORTROUTE'] in (211, 221, 231, 219):
    row['SAMP_PORT_GRP_PV'] = 'AME'
elif (Irish) and row['PORTROUTE'] in (241, 249):
    row['SAMP_PORT_GRP_PV'] = 'ALE'
elif (Irish) and row['PORTROUTE'] in (201, 202):
    row['SAMP_PORT_GRP_PV'] = 'ASE'
elif (Irish) and (row['PORTROUTE'] >= 300) and (row['PORTROUTE'] < 600):
    row['SAMP_PORT_GRP_PV'] = 'ARE'
elif (ChannelI) and (row['PORTROUTE'] >= 100) and (row['PORTROUTE'] < 300):
    row['SAMP_PORT_GRP_PV'] = 'MAC'
elif (ChannelI) and (row['PORTROUTE'] >= 300) and (row['PORTROUTE'] < 600):
    row['SAMP_PORT_GRP_PV'] = 'RAC'
elif (IoM) and (row['PORTROUTE'] >= 100) and (row['PORTROUTE'] < 300):
    row['SAMP_PORT_GRP_PV'] = 'MAM'
elif (IoM) and (row['PORTROUTE'] >= 300) and (row['PORTROUTE'] < 600):
    row['SAMP_PORT_GRP_PV'] = 'RAM'

if row['SAMP_PORT_GRP_PV'] == 'HGS':
    row['SAMP_PORT_GRP_PV'] = 'HBN'

if row['SAMP_PORT_GRP_PV'] == 'EGS':
    row['SAMP_PORT_GRP_PV'] = 'HBN'

if row['SAMP_PORT_GRP_PV'] == 'MAM':
    row['SAMP_PORT_GRP_PV'] = 'MAC'

if row['SAMP_PORT_GRP_PV'] == 'RAM':
    row['SAMP_PORT_GRP_PV'] = 'RAC'

if row['SAMP_PORT_GRP_PV'] == 'A331' and (row['ARRIVEDEPART'] == 1):
    row['SAMP_PORT_GRP_PV'] = 'A391'
if row['SAMP_PORT_GRP_PV'] == 'A331' and (row['ARRIVEDEPART'] == 2):
    row['SAMP_PORT_GRP_PV'] = 'A391'

if row['SAMP_PORT_GRP_PV'] == 'A401' and (row['ARRIVEDEPART'] == 1):
    row['SAMP_PORT_GRP_PV'] = 'A441'
if row['SAMP_PORT_GRP_PV'] == 'A401' and (row['ARRIVEDEPART'] == 2):
    row['SAMP_PORT_GRP_PV'] = 'A441'

if row['SAMP_PORT_GRP_PV'] == 'SLR' and (row['ARRIVEDEPART'] == 1):
    row['SAMP_PORT_GRP_PV'] = 'SCF'
if row['SAMP_PORT_GRP_PV'] == 'SLR' and (row['ARRIVEDEPART'] == 2):
    row['SAMP_PORT_GRP_PV'] = 'SCF'

if row['SAMP_PORT_GRP_PV'] == 'SSE' and (row['ARRIVEDEPART'] == 1):
    row['SAMP_PORT_GRP_PV'] = 'SNE'"""

mins_port_grp_pv = "row['MINS_PORT_GRP_PV'] = int(row['PORTROUTE'])"

mins_ctry_grp_pv = """row['MINS_CTRY_GRP_PV'] = row['FLOW']"""

reg_imp_eligible_pv = """if row['FLOW'] in (1,5) and row['RESPNSE'] != 5 and (row['PURPOSE'] <= 89 or row['PURPOSE'] == 92 or math.isnan(row['PURPOSE'])):
    row['REG_IMP_ELIGIBLE_PV'] = 1
else:
    row['REG_IMP_ELIGIBLE_PV'] = 0"""

town_imp_eligible_pv = """if row['FLOW'] in (1,5) and row['RESPNSE'] != 5 and (row['PURPOSE'] <= 89 or row['PURPOSE'] == 92 or math.isnan(row['PURPOSE'])):
    row['TOWN_IMP_ELIGIBLE_PV'] = 1
else:
    row['TOWN_IMP_ELIGIBLE_PV'] = 0"""

purpose_pv = """if row['PURPOSE'] == 3 or row['PURPOSE'] == 4 or row['PURPOSE'] == 31 or row['PURPOSE'] == 32:
    row['PURPOSE_PV'] = 1
elif row['PURPOSE'] == 1 or row['PURPOSE'] == 2:
    row['PURPOSE_PV'] = 2
elif row['PURPOSE'] == 5:
    row['PURPOSE_PV'] = 3
elif row['PURPOSE'] == 61 or row['PURPOSE'] == 62 or row['PURPOSE'] == 6:
    row['PURPOSE_PV'] = 4
else:
    row['PURPOSE_PV'] = 5"""

spend_imp_flag_pv = """if math.isnan(row['SPEND']):
    row['SPEND_IMP_FLAG_PV'] = 1
else:
    row['SPEND_IMP_FLAG_PV'] = 0"""

rail_imp_eligible_pv = """if row['FLOW'] in (5,8):
    row['RAIL_IMP_ELIGIBLE_PV'] = 1
else:
    row['RAIL_IMP_ELIGIBLE_PV'] = 0"""

rail_exercise_pv = """if row['FLOW'] == 8:
    if row['RAIL_CNTRY_GRP_PV'] == 1:
        row['RAIL_EXERCISE_PV'] = 38
    elif row['RAIL_CNTRY_GRP_PV'] == 2:
        row['RAIL_EXERCISE_PV'] = 59
    elif row['RAIL_CNTRY_GRP_PV'] == 3:
        row['RAIL_EXERCISE_PV'] = 25
    elif row['RAIL_CNTRY_GRP_PV'] == 4:
        row['RAIL_EXERCISE_PV'] = 36
    elif row['RAIL_CNTRY_GRP_PV'] == 5:
        row['RAIL_EXERCISE_PV'] = 43
    elif row['RAIL_CNTRY_GRP_PV'] == 6:
        row['RAIL_EXERCISE_PV'] = 4
    elif row['RAIL_CNTRY_GRP_PV'] == 7:
        row['RAIL_EXERCISE_PV'] = 28
    elif row['RAIL_CNTRY_GRP_PV'] == 8:
        row['RAIL_EXERCISE_PV'] = 65
    elif row['RAIL_CNTRY_GRP_PV'] == 9:
        row['RAIL_EXERCISE_PV'] = 0
    elif row['RAIL_CNTRY_GRP_PV'] == 10:
        row['RAIL_EXERCISE_PV'] = 0
    elif row['RAIL_CNTRY_GRP_PV'] == 11:
        row['RAIL_EXERCISE_PV'] = 0
    elif row['RAIL_CNTRY_GRP_PV'] == 12:
        row['RAIL_EXERCISE_PV'] = 28
elif row['FLOW'] == 5:
    if row['RAIL_CNTRY_GRP_PV'] == 1:
        row['RAIL_EXERCISE_PV'] = 137
    elif row['RAIL_CNTRY_GRP_PV'] == 2:
        row['RAIL_EXERCISE_PV'] = 146
    elif row['RAIL_CNTRY_GRP_PV'] == 3:
        row['RAIL_EXERCISE_PV'] = 23
    elif row['RAIL_CNTRY_GRP_PV'] == 4:
        row['RAIL_EXERCISE_PV'] = 304
    elif row['RAIL_CNTRY_GRP_PV'] == 5:
        row['RAIL_EXERCISE_PV'] = 4
    elif row['RAIL_CNTRY_GRP_PV'] == 6:
        row['RAIL_EXERCISE_PV'] = 36
    elif row['RAIL_CNTRY_GRP_PV'] == 7:
        row['RAIL_EXERCISE_PV'] = 67
    elif row['RAIL_CNTRY_GRP_PV'] == 8:
        row['RAIL_EXERCISE_PV'] = 9
    elif row['RAIL_CNTRY_GRP_PV'] == 9:
        row['RAIL_EXERCISE_PV'] = 0
    elif row['RAIL_CNTRY_GRP_PV'] == 10:
        row['RAIL_EXERCISE_PV'] = 0
    elif row['RAIL_CNTRY_GRP_PV'] == 11:
        row['RAIL_EXERCISE_PV'] = 0
    elif row['RAIL_CNTRY_GRP_PV'] == 12:
        row['RAIL_EXERCISE_PV'] = 23

row['RAIL_EXERCISE_PV'] = row['RAIL_EXERCISE_PV'] * 1000"""

osport1_pv = """row['OSPORT1_PV'] = row['DVPORTCODE']

if not math.isnan(row['CHANGECODE']):
    row['OSPORT1_PV'] = row['CHANGECODE']

if row['OSPORT1_PV'] in (999998,999999):
    row['OSPORT1_PV'] = float('NaN')"""

ukport4_pv = """if row['UKPORT3_PV'] in (1,2,3,9):
    row['UKPORT4_PV'] = 1
elif row['UKPORT3_PV'] in (4,5,6,7,8):
    row['UKPORT4_PV'] = 2
else:
    row['UKPORT4_PV'] = 0"""

ukport3_pv = """if row['UKPORT2_PV'] in (1,2,4,13):
    row['UKPORT3_PV'] = 1
elif row['UKPORT2_PV'] in (3,10,11):
    row['UKPORT3_PV'] = 2
elif row['UKPORT2_PV'] in (12,14):
    row['UKPORT3_PV'] = 3
elif row['UKPORT2_PV'] in (21,22):
    row['UKPORT3_PV'] = 4
elif row['UKPORT2_PV'] in (23,24):
    row['UKPORT3_PV'] = 5
elif row['UKPORT2_PV'] in (25,26):
    row['UKPORT3_PV'] = 6
elif row['UKPORT2_PV'] == 27:
    row['UKPORT3_PV'] = 7
elif row['UKPORT2_PV'] == 31:
    row['UKPORT3_PV'] = 8
elif row['UKPORT2_PV'] == 32:
    row['UKPORT3_PV'] = 9
else:
    row['UKPORT3_PV'] = 0"""

ukport2_pv = """if row['UKPORT1_PV'] >= 110 and row['UKPORT1_PV'] <= 150:
    row['UKPORT2_PV'] = 1
elif row['UKPORT1_PV'] in (180,190):
    row['UKPORT2_PV'] = 2
elif (row['UKPORT1_PV'] >= 210 and row['UKPORT1_PV'] <= 231):
    row['UKPORT2_PV'] = 3
elif row['UKPORT1_PV'] == 200:
    row['UKPORT2_PV'] = 4
elif row['UKPORT1_PV'] == 340:
    row['UKPORT2_PV'] = 5
elif row['UKPORT1_PV'] in (381,391,451):
    row['UKPORT2_PV'] = 10
elif row['UKPORT1_PV'] in (401,411,441):
    row['UKPORT2_PV'] = 11
elif row['UKPORT1_PV'] in (310,371):
    row['UKPORT2_PV'] = 12
elif row['UKPORT1_PV'] == 421:
    row['UKPORT2_PV'] = 13
elif row['UKPORT1_PV'] in (351,361):
    row['UKPORT2_PV'] = 14
elif row['UKPORT1_PV'] in (461,481):
    row['UKPORT2_PV'] = 15
elif row['UKPORT1_PV'] in (611,612):
    row['UKPORT2_PV'] = 21
elif row['UKPORT1_PV'] in (631,632,633,634) or (row['UKPORT1_PV'] >= 651 and row['UKPORT1_PV'] <= 662):
    row['UKPORT2_PV'] = 22
elif row['UKPORT1_PV'] in (671, 672):
    row['UKPORT2_PV'] = 23
elif row['UKPORT1_PV'] >= 681 and row['UKPORT1_PV'] <= 692:
    row['UKPORT2_PV'] = 24
elif row['UKPORT1_PV'] in (701,711):
    row['UKPORT2_PV'] = 25
elif row['UKPORT1_PV'] in (721,722):
    row['UKPORT2_PV'] = 26
elif row['UKPORT1_PV'] == 641:
    row['UKPORT2_PV'] = 27
elif row['UKPORT1_PV'] in (811,812):
    row['UKPORT2_PV'] = 31
elif row['UKPORT1_PV'] >= 911 and row['UKPORT1_PV'] <= 951:
    row['UKPORT2_PV'] = 32
else:
    row['UKPORT2_PV'] = 99"""

ukport1_pv = """if (row['PORTROUTE'] >= 111 and row['PORTROUTE'] <= 119) or row['PORTROUTE'] in (161,171):
    row['UKPORT1_PV'] = 110
elif (row['PORTROUTE'] >= 121 and row['PORTROUTE'] <= 129) or row['PORTROUTE'] in (162,172):
    row['UKPORT1_PV'] = 120
elif (row['PORTROUTE'] >= 131 and row['PORTROUTE'] <= 139) or row['PORTROUTE'] in (163,173):
    row['UKPORT1_PV'] = 130
elif (row['PORTROUTE'] >= 141 and row['PORTROUTE'] <= 149) or row['PORTROUTE'] in (164,174):
    row['UKPORT1_PV'] = 140
elif (row['PORTROUTE'] >= 151 and row['PORTROUTE'] <= 159) or row['PORTROUTE'] in (165,175):
    row['UKPORT1_PV'] = 150
elif row['PORTROUTE'] >= 181 and row['PORTROUTE'] <= 189:
    row['UKPORT1_PV'] = 180
elif row['PORTROUTE'] >= 191 and row['PORTROUTE'] <= 199:
    row['UKPORT1_PV'] = 190
elif row['PORTROUTE'] >= 201 and row['PORTROUTE'] <= 209:
    row['UKPORT1_PV'] = 200
elif row['PORTROUTE'] >= 211 and row['PORTROUTE'] <= 219:
    row['UKPORT1_PV'] = 210
elif row['PORTROUTE'] >= 221 and row['PORTROUTE'] <= 229:
    row['UKPORT1_PV'] = 220
elif row['PORTROUTE'] >= 241 and row['PORTROUTE'] <= 249:
    row['UKPORT1_PV'] = 240
elif row['PORTROUTE'] >= 311 and row['PORTROUTE'] <= 319:
    row['UKPORT1_PV'] = 310
else:
    row['UKPORT1_PV'] = row['PORTROUTE']"""

StayImpCtryLevel2_pv = """if row['STAYIMPCTRYLEVEL1_PV'] in (830, 831, 832, 833, 931, 372):
    row['STAYIMPCTRYLEVEL2_PV'] = 1
elif row['STAYIMPCTRYLEVEL1_PV'] in (250, 56, 442, 528, 492):
    row['STAYIMPCTRYLEVEL2_PV'] = 2
elif row['STAYIMPCTRYLEVEL1_PV'] in (620, 621, 911, 912, 20, 292):
    row['STAYIMPCTRYLEVEL2_PV'] = 3
elif row['STAYIMPCTRYLEVEL1_PV'] in (276, 40, 756, 438, 208):
    row['STAYIMPCTRYLEVEL2_PV'] = 4
elif row['STAYIMPCTRYLEVEL1_PV'] in (470, 901, 902, 380, 792, 300, 674):
    row['STAYIMPCTRYLEVEL2_PV'] = 5
elif row['STAYIMPCTRYLEVEL1_PV'] in (352, 248, 246, 578, 744, 752, 234):
    row['STAYIMPCTRYLEVEL2_PV'] = 6
elif row['STAYIMPCTRYLEVEL1_PV'] in (70, 191, 807, 499, 951, 688, 705, 100, 642, 203, 703):
    row['STAYIMPCTRYLEVEL2_PV'] = 7
elif row['STAYIMPCTRYLEVEL1_PV'] in (348, 616, 8, 643, 51, 31, 112, 233, 268, 398, 417, 428, 440, 498, 762, 795, 804, 860):
    row['STAYIMPCTRYLEVEL2_PV'] = 8
elif row['STAYIMPCTRYLEVEL1_PV'] in (12, 434, 504, 736, 788, 818, 732):
    row['STAYIMPCTRYLEVEL2_PV'] = 11
elif row['STAYIMPCTRYLEVEL1_PV'] in (270, 288, 566, 694, 654, 404, 426, 454, 480,690, 834, 800, 894, 72, 716, 204, 266, 324,624, 384, 430, 466, 478, 562, 686, 768, 854, 24, 108, 120, 140, 148, 178, 180, 231, 450, 508, 646, 706, 262, 10, 132, 174, 226, 260, 175, 638, 678, 232):
    row['STAYIMPCTRYLEVEL2_PV'] = 12
elif row['STAYIMPCTRYLEVEL1_PV'] in (748, 710, 516):
    row['STAYIMPCTRYLEVEL2_PV'] = 13
elif row['STAYIMPCTRYLEVEL1_PV'] in (400, 376, 275, 422, 887):
    row['STAYIMPCTRYLEVEL2_PV'] = 14
elif row['STAYIMPCTRYLEVEL1_PV'] in (48, 414, 512, 634, 784, 364, 368, 682, 760):
    row['STAYIMPCTRYLEVEL2_PV'] = 15
elif row['STAYIMPCTRYLEVEL1_PV'] in (462, 50, 144, 356, 586):
    row['STAYIMPCTRYLEVEL2_PV'] = 21
elif row['STAYIMPCTRYLEVEL1_PV'] in (344, 156, 496, 524):
    row['STAYIMPCTRYLEVEL2_PV'] = 22
elif row['STAYIMPCTRYLEVEL1_PV'] in (96, 458, 360, 608):
    row['STAYIMPCTRYLEVEL2_PV'] = 23
elif row['STAYIMPCTRYLEVEL1_PV'] in (702, 392, 158, 764):
    row['STAYIMPCTRYLEVEL2_PV'] = 24
elif row['STAYIMPCTRYLEVEL1_PV'] in (4, 104, 116, 410, 418, 446, 704, 626, 408):
    row['STAYIMPCTRYLEVEL2_PV'] = 25
elif row['STAYIMPCTRYLEVEL1_PV'] in (334, 36, 554):
    row['STAYIMPCTRYLEVEL2_PV'] = 31
elif row['STAYIMPCTRYLEVEL1_PV'] in (242, 598, 258, 296, 316, 581, 580, 584, 583, 16, 772, 581, 540, 74, 86, 162, 166, 184, 798, 520, 570, 574, 585, 612, 882, 90, 776, 798, 548, 876):
    row['STAYIMPCTRYLEVEL2_PV'] = 32
elif row['STAYIMPCTRYLEVEL1_PV'] in (124, 666, 304):
    row['STAYIMPCTRYLEVEL2_PV'] = 41
elif row['STAYIMPCTRYLEVEL1_PV'] in (840, 630, 850):
    row['STAYIMPCTRYLEVEL2_PV'] = 42
elif row['STAYIMPCTRYLEVEL1_PV'] in (60, 388, 780, 28, 44, 52, 92, 136, 212, 308, 500, 660, 659, 662, 670, 796, 192, 214, 312, 652, 663, 332, 474, 530, 533, 84, 328):
    row['STAYIMPCTRYLEVEL2_PV'] = 43
elif row['STAYIMPCTRYLEVEL1_PV'] in (484, 340, 320,222, 558, 188, 862, 591):
    row['STAYIMPCTRYLEVEL2_PV'] = 44
elif row['STAYIMPCTRYLEVEL1_PV'] in (76, 68, 152, 170, 218, 604, 858, 862, 254, 740):
    row['STAYIMPCTRYLEVEL2_PV'] = 45
elif row['STAYIMPCTRYLEVEL1_PV'] in (238, 32, 858, 600, 152):
    row['STAYIMPCTRYLEVEL2_PV'] = 46
elif row['STAYIMPCTRYLEVEL1_PV'] in (40, 42, 43, 44):
    row['STAYIMPCTRYLEVEL2_PV'] = 51
elif row['STAYIMPCTRYLEVEL1_PV'] in (41,45, 46, 47, 49):
    row['STAYIMPCTRYLEVEL2_PV'] = 52
elif row['STAYIMPCTRYLEVEL1_PV'] in (0, 969, 99):
    row['STAYIMPCTRYLEVEL2_PV'] = 91
else:
    row['STAYIMPCTRYLEVEL2_PV'] = 99"""

StayImpCtryLevel1_pv = """if row['UKFOREIGN'] == 1:
    if not math.isnan(row['COUNTRYVISIT']):
        row['STAYIMPCTRYLEVEL1_PV'] = int(row['COUNTRYVISIT'])
if row['UKFOREIGN'] == 2:
    if not math.isnan(row['RESIDENCE']):
        row['STAYIMPCTRYLEVEL1_PV'] = int(row['RESIDENCE'])
"""

imbal_ctry_fact_pv = """if row['RESIDENCE'] == 352 or row['RESIDENCE'] == 40 or row['RESIDENCE'] in (292, 470, 902, 901) or row['RESIDENCE'] == 792 or row['RESIDENCE'] == 620 or row['RESIDENCE'] == 621 or row['RESIDENCE'] in (973, 70, 191, 807, 499, 688, 951, 705)  or row['RESIDENCE'] == 234:
    row['IMBAL_CTRY_FACT_PV'] = 1.02
elif row['RESIDENCE'] == 56 or row['RESIDENCE'] == 442:
    row['IMBAL_CTRY_FACT_PV'] = 0.9
elif (row['RESIDENCE'] == 250) or row['RESIDENCE'] == 492:
    row['IMBAL_CTRY_FACT_PV'] = 1.12
elif row['RESIDENCE'] == 276:
    row['IMBAL_CTRY_FACT_PV'] = 0.9
elif (row['RESIDENCE'] == 380) or row['RESIDENCE'] == 674:
    row['IMBAL_CTRY_FACT_PV'] = 0.9
elif row['RESIDENCE'] == 528:
    row['IMBAL_CTRY_FACT_PV'] = 0.98
elif row['RESIDENCE'] == 208:
    row['IMBAL_CTRY_FACT_PV'] = 1.08
elif row['RESIDENCE'] in (246, 248, 578, 744, 752):
    row['IMBAL_CTRY_FACT_PV'] = 0.86
elif (row['RESIDENCE'] == 300):
    row['IMBAL_CTRY_FACT_PV'] = 1.06
elif (row['RESIDENCE'] == 911) or row['RESIDENCE'] == 20 or row['RESIDENCE'] == 732:
    row['IMBAL_CTRY_FACT_PV'] = 1.16
elif row['RESIDENCE'] == 756 or row['RESIDENCE'] == 438:
    row['IMBAL_CTRY_FACT_PV'] = 1.04
elif row['RESIDENCE'] in (100, 642, 203, 703, 348, 616, 8, 643, 51, 31, 112, 233, 268, 398, 417, 428, 440, 498, 762, 795, 804, 860):
    row['IMBAL_CTRY_FACT_PV'] = 1.14
elif row['RESIDENCE'] in (12, 434, 504, 736, 788, 818) or row['RESIDENCE'] in (48, 400, 414, 512, 634, 784, 275, 376, 364, 368, 422, 682, 887, 760):
    row['IMBAL_CTRY_FACT_PV'] = 1.1
elif row['RESIDENCE'] in (270, 288, 566, 694, 654, 404, 426, 454, 480, 690, 834, 800, 894, 72, 748) or row['RESIDENCE'] in (204, 266, 324, 624, 384, 430, 466, 478, 562, 686, 768, 854, 24, 108, 120, 140, 148, 178, 180, 231, 450, 450, 508, 646, 706, 262, 10, 132, 174, 175, 226, 226, 260, 638, 678, 232) or row['RESIDENCE'] in (156, 408, 496):
    row['IMBAL_CTRY_FACT_PV'] = 1.0
elif row['RESIDENCE'] in (710, 516):
    row['IMBAL_CTRY_FACT_PV'] = 0.96
elif row['RESIDENCE'] in (36, 334, 554):
    row['IMBAL_CTRY_FACT_PV'] = 1.0
elif row['RESIDENCE'] in (242, 598, 598, 258, 16, 296, 316, 580, 581, 583, 584, 772, 540, 74, 86, 90, 162, 166, 184, 296, 520, 548, 570, 574, 585, 612, 776, 798, 876, 882) or row['RESIDENCE'] in (50, 96, 144, 458, 702) or (row['RESIDENCE'] == 356) or row['RESIDENCE'] in (586, 4, 64, 104, 116, 360, 410, 418, 446, 524, 608, 158, 764, 704, 626):
    row['IMBAL_CTRY_FACT_PV'] = 1.1
elif row['RESIDENCE'] == 344:
    row['IMBAL_CTRY_FACT_PV'] = 1.02
elif (row['RESIDENCE'] == 392):
    row['IMBAL_CTRY_FACT_PV'] = 1.16
elif row['RESIDENCE'] in (60, 388, 780, 28, 44, 52, 92, 136, 212, 308, 500, 659, 660, 662, 670, 796) or row['RESIDENCE'] in (84, 328, 238, 239):
    row['IMBAL_CTRY_FACT_PV'] = 1.02
elif row['RESIDENCE'] in (192, 214, 312, 652, 663, 332, 474, 530, 533) or row['RESIDENCE'] in (32, 76, 484, 68, 152, 170, 218, 600, 604, 858, 862, 188, 222, 320, 340, 558, 254, 591, 740):
    row['IMBAL_CTRY_FACT_PV'] = 1.1
elif row['RESIDENCE'] == 124 or row['RESIDENCE'] in (666, 304):
    row['IMBAL_CTRY_FACT_PV'] = 1.04
elif row['RESIDENCE'] in (840, 630, 850):
    row['IMBAL_CTRY_FACT_PV'] = 1.04
else:
    row['IMBAL_CTRY_FACT_PV'] = 1.0"""

dur2_pv = """if row['STAY'] >= 0 and row['STAY'] <= 30:
    row['DUR2_PV'] = 1
elif row['STAY'] >= 31:
    row['DUR2_PV'] = 2"""

dur1_pv = """if row['STAY'] == 0:
    row['DUR1_PV'] = 0
elif row['STAY'] >= 1 and row['STAY'] <= 7:
    row['DUR1_PV'] = 1
elif row['STAY'] >= 8 and row['STAY'] <= 21:
    row['DUR1_PV'] = 2
elif row['STAY'] >= 22 and row['STAY'] <= 35:
    row['DUR1_PV'] = 3
elif row['STAY'] >= 36 and row['STAY'] <= 91:
    row['DUR1_PV'] = 4
elif row['STAY'] >= 92:
    row['DUR1_PV'] = 5"""

pur3_pv = """if row['PURPOSE'] in (20,21,22):
    row['PUR3_PV'] = 1
elif math.isnan(row['PURPOSE']):
    row['PUR3_PV'] = None
else:
    row['PUR3_PV'] = 2"""

pur2_pv = """if row['PURPOSE'] in (10,14,17,18,11,12):
    row['PUR2_PV'] = 2
elif row['PURPOSE'] in (20,21,22):
    row['PUR2_PV'] = 3
elif row['PURPOSE'] == 71:
    row['PUR2_PV'] = 4
elif math.isnan(row['PURPOSE']):
    row['PUR2_PV'] = None
else:
    row['PUR2_PV'] = 5

if row['IND'] == 1 and row['PUR2_PV'] == 2:
    row['PUR2_PV'] = 1"""

pur1_pv = """if row['DVPACKAGE'] in (1,2):
    row['IND'] = 1

if row['DVPACKAGE'] == 9 or math.isnan(row['DVPACKAGE']):
    row['IND'] = 0

if row['PURPOSE'] in (10,14,17,18):
    row['PUR1_PV'] = 2
elif row['PURPOSE'] in (20,21,22):
    row['PUR1_PV'] = 3
elif row['PURPOSE'] in (11,12):
    row['PUR1_PV'] = 4
elif row['PURPOSE'] == 40:
    row['PUR1_PV'] = 5
elif row['PURPOSE'] == 71:
    row['PUR1_PV'] = 6
else:
    row['PUR1_PV'] = 7

if row['IND'] == 1 and row['PUR1_PV'] == 2:
    row['PUR1_PV'] = 1
"""

uk_os_pv = """if row['FLOW'] in (1,5):
    row['UK_OS_PV'] = 2

if row['FLOW'] in (4,8):
    row['UK_OS_PV'] = 1"""

spend_imp_eligible_pv = """if (row['FLOW'] in (1,4,5,8) and row['PURPOSE'] < 80 and row['PURPOSE'] != 23 and row['PURPOSE'] != 24 and row['MINS_FLAG_PV'] == 0) or (row['FLOW'] in (1,4,5,8) and str(row['PURPOSE']) == 'nan' and row['MINS_FLAG_PV'] == 0):
    row['SPEND_IMP_ELIGIBLE_PV'] = 1
else:
    row['SPEND_IMP_ELIGIBLE_PV'] = 0"""

duty_free_pv = """if row['FLOW'] == 1 and ((row['PURPOSE'] < 80 and row['PURPOSE'] != 71) or math.isnan(row['PURPOSE'])):
    row['DUTY_FREE_PV'] = 15
else:
    row['DUTY_FREE_PV'] = 0"""

qmfare_pv = """if row['OSPORT3_PV'] == 12 and (row['MINS_FLAG_PV'] == 0 or math.isnan(row['MINS_FLAG_PV'])):
    row['QMFARE_PV'] = 1500
else:
    row['QMFARE_PV'] = None"""

apd_pv = """if row['OSPORT2_PV'] in (210,500,600,700,800):
    APDBAND = 1
elif row['OSPORT2_PV'] in (1000,1100,1200,1700,2000):
    APDBAND = 1
elif row['OSPORT2_PV'] in (2100,2200,2300,2390,2500):
    APDBAND = 1
elif row['OSPORT2_PV'] in (2590,2800,2830,2840,150,160):
    APDBAND = 1
elif row['OSPORT2_PV'] in (310,320,340,2760,3020,3030):
    APDBAND = 1
elif row['OSPORT2_PV'] in (3040,3050,3060,3130,3170,3180):
    APDBAND = 1
elif row['OSPORT2_PV'] in (3000,3010):
    APDBAND = 1
else:
    APDBAND = 2

if row['FLOW'] > 4:
    row['APD_PV'] = 0
elif APDBAND == 1:
    row['APD_PV'] = 10/2
else:
    row['APD_PV'] = 40/2"""

osport4_pv = """if row['OSPORT3_PV'] in (1,2):
    row['OSPORT4_PV'] = 1
else:
    row['OSPORT4_PV'] = 2"""

osport3_pv = """if row['OSPORT2_PV'] in (40,56,250,276,372,438,442,492,528,756,830,831,832,833,921,922,923,924,926,931):
    row['OSPORT3_PV'] = 1
elif row['OSPORT2_PV'] in (208,233,234,246,248,352,428,440,578,744,752):
    row['OSPORT3_PV'] = 2
elif row['OSPORT2_PV'] in (31,51,112,203,268,348,498,616,642,643,703,804):
    row['OSPORT3_PV'] = 3
elif row['OSPORT2_PV'] in (8,20,70,100,191,292,300,336,380,470,499,620,621,674,688,705,792,807,901,902,911,912,951,973):
    row['OSPORT3_PV'] = 4
elif row['OSPORT2_PV'] in (12,434,504,732,736,788,818):
    row['OSPORT3_PV'] = 5
elif row['OSPORT2_PV'] in (24,72,108,120,132,140,148,174,175,178,180,204,226,231,232,262,266,270,288,324,384,404,426,430,450,454,466,478,480,508,516,562,566,624,638,646,654,678,686,690,694,706,710,716,748,768,800,834,854,894):
    row['OSPORT3_PV'] = 6
elif row['OSPORT2_PV'] in (60,124,304,840):
    row['OSPORT3_PV'] = 7
elif row['OSPORT2_PV'] in (28,32,44,52,68,76,84,92,136,152,170,188,192,212,214,218,222,238,254,308,312,320,328,332,340,388,474,484,500,530,533,558,591,600,604,630,652,659,660,662,663,666,670,740,780,796,850,858,862):
    row['OSPORT3_PV'] = 8
elif row['OSPORT2_PV'] in (4,50,64,96,104,116,144,156,158,344,356,360,392,398,408,410,417,418,446,458,462,496,524,586,608,626,702,704,762,764,795,860):
    row['OSPORT3_PV'] = 9
elif row['OSPORT2_PV'] in (48,275,364,368,376,400,414,422,512,634,682,760,784,887):
    row['OSPORT3_PV'] = 10
elif row['OSPORT2_PV'] in (10,16,36,74,86,90,162,166,184,239,242,258,260,296,316,334,520,540,548,554,570,574,580,581,583,584,585,598,612,772,776,798,876,882):
    row['OSPORT3_PV'] = 11
elif row['OSPORT2_PV'] in (940,941,942,943,944,945,946,947,949):
    row['OSPORT3_PV'] = 12
else:
    row['OSPORT3_PV'] = 13"""

rail_cntry_grp_pv = """railcountry = 0

if row['FLOW'] == 5:
    railcountry = row['RESIDENCE']
elif row['FLOW'] == 8:
    railcountry = row['COUNTRYVISIT']

if (railcountry == 250):
    row['RAIL_CNTRY_GRP_PV'] = 1
elif railcountry in (208,578,752):
    row['RAIL_CNTRY_GRP_PV'] = 2
elif railcountry == 56:
    row['RAIL_CNTRY_GRP_PV'] = 3
elif railcountry == 276:
    row['RAIL_CNTRY_GRP_PV'] = 4
elif railcountry == 380:
    row['RAIL_CNTRY_GRP_PV'] = 5
elif railcountry in (911,912):
    row['RAIL_CNTRY_GRP_PV'] = 6
elif railcountry == 756:
    row['RAIL_CNTRY_GRP_PV'] = 7
elif railcountry in (40,442,528,620):
    row['RAIL_CNTRY_GRP_PV'] = 8
elif railcountry == 372:
    row['RAIL_CNTRY_GRP_PV'] = 9
elif railcountry == 840:
    row['RAIL_CNTRY_GRP_PV'] = 10
elif railcountry == 124:
    row['RAIL_CNTRY_GRP_PV'] = 11
elif railcountry in (112,100,191,203,246,300,348,973,428,440,807,504,616,642,643,703,705,792,804,233):
    row['RAIL_CNTRY_GRP_PV'] = 12"""

osport2_pv = """if row['UKPORT1_PV'] == 641:
    if not math.isnan(row['OSPORT1_PV']):
        row['OSPORT2_PV'] = int(float(row['OSPORT1_PV']) / 100.0)
else:
    if not math.isnan(row['OSPORT1_PV']):
        row['OSPORT2_PV'] = int(float(row['OSPORT1_PV']) / 1000.0)

if row['UKFOREIGN'] == 1 and math.isnan(row['OSPORT1_PV']):
    row['OSPORT2_PV'] = row['COUNTRYVISIT']

if row['UKFOREIGN'] == 2 and math.isnan(row['OSPORT1_PV']):
    row['OSPORT2_PV'] = row['RESIDENCE']

if row['OSPORT2_PV'] == 300:
    row['OSPORT2_PV'] = 2500
elif row['OSPORT2_PV'] == 310:
    row['OSPORT2_PV'] = 2200
elif row['OSPORT2_PV'] == 320:
    row['OSPORT2_PV'] = 1000
elif row['OSPORT2_PV'] == 150:
    row['OSPORT2_PV'] = 210
elif row['OSPORT2_PV'] == 160:
    row['OSPORT2_PV'] = 210"""

stay_imp_eligible_pv = """if row['FLOW'] in (1,4,5,8) and row['MINS_FLAG_PV'] == 0 and row['PURPOSE'] != 80:
    row['STAY_IMP_ELIGIBLE_PV'] = 1
else:
    row['STAY_IMP_ELIGIBLE_PV'] = 0"""

stay_imp_flag_pv = """if math.isnan(row['NUMNIGHTS']) or row['NUMNIGHTS'] == 999:
    row['STAY_IMP_FLAG_PV'] = 1
else:
    row['STAY_IMP_FLAG_PV'] = 0"""

imbal_port_fact_pv = """if row['IMBAL_PORT_GRP_PV'] == 1 and row['ARRIVEDEPART'] == 1:
    row['IMBAL_PORT_FACT_PV'] = 1.00
elif row['IMBAL_PORT_GRP_PV'] == 1 and row['ARRIVEDEPART'] == 2:
    row['IMBAL_PORT_FACT_PV'] = 0.99
elif row['IMBAL_PORT_GRP_PV'] == 2 and row['ARRIVEDEPART'] == 1:
    row['IMBAL_PORT_FACT_PV'] = 1.0
elif row['IMBAL_PORT_GRP_PV'] == 2 and row['ARRIVEDEPART'] == 2:
    row['IMBAL_PORT_FACT_PV'] = 0.99
elif row['IMBAL_PORT_GRP_PV'] == 3 and row['ARRIVEDEPART'] == 1:
    row['IMBAL_PORT_FACT_PV'] = 1.00
elif row['IMBAL_PORT_GRP_PV'] == 3 and row['ARRIVEDEPART'] == 2:
    row['IMBAL_PORT_FACT_PV'] = 1.00
elif row['IMBAL_PORT_GRP_PV'] == 4 and row['ARRIVEDEPART'] == 1:
    row['IMBAL_PORT_FACT_PV'] = 1.0
elif row['IMBAL_PORT_GRP_PV'] == 4 and row['ARRIVEDEPART'] == 2:
    row['IMBAL_PORT_FACT_PV'] = 1.0
elif row['IMBAL_PORT_GRP_PV'] == 5 and row['ARRIVEDEPART'] == 1:
    row['IMBAL_PORT_FACT_PV'] = 1.0
elif row['IMBAL_PORT_GRP_PV'] == 5 and row['ARRIVEDEPART'] == 2:
    row['IMBAL_PORT_FACT_PV'] = 1.0
elif row['IMBAL_PORT_GRP_PV'] == 6 and row['ARRIVEDEPART'] == 1:
    row['IMBAL_PORT_FACT_PV'] = 0.99
elif row['IMBAL_PORT_GRP_PV'] == 6 and row['ARRIVEDEPART'] == 2:
    row['IMBAL_PORT_FACT_PV'] = 1.04
elif row['IMBAL_PORT_GRP_PV'] == 7 and row['ARRIVEDEPART'] == 1:
    row['IMBAL_PORT_FACT_PV'] = 0.98
elif row['IMBAL_PORT_GRP_PV'] == 7 and row['ARRIVEDEPART'] == 2:
    row['IMBAL_PORT_FACT_PV'] = 1.03
elif row['IMBAL_PORT_GRP_PV'] == 8 and row['ARRIVEDEPART'] == 1:
    row['IMBAL_PORT_FACT_PV'] = 0.96
elif row['IMBAL_PORT_GRP_PV'] == 8 and row['ARRIVEDEPART'] == 2:
    row['IMBAL_PORT_FACT_PV'] = 1.04
elif row['IMBAL_PORT_GRP_PV'] == 9 and row['ARRIVEDEPART'] == 1:
    row['IMBAL_PORT_FACT_PV'] = 0.97
elif row['IMBAL_PORT_GRP_PV'] == 9 and row['ARRIVEDEPART'] == 2:
    row['IMBAL_PORT_FACT_PV'] = 1.04
elif row['IMBAL_PORT_GRP_PV'] == 10 and row['ARRIVEDEPART'] == 1:
    row['IMBAL_PORT_FACT_PV'] = 0.98
elif row['IMBAL_PORT_GRP_PV'] == 10 and row['ARRIVEDEPART'] == 2:
    row['IMBAL_PORT_FACT_PV'] = 1.0
elif row['IMBAL_PORT_GRP_PV'] == 11 and row['ARRIVEDEPART'] == 1:
    row['IMBAL_PORT_FACT_PV'] = 0.96
elif row['IMBAL_PORT_GRP_PV'] == 11 and row['ARRIVEDEPART'] == 2:
    row['IMBAL_PORT_FACT_PV'] = 1.05
elif row['IMBAL_PORT_GRP_PV'] == 12 and row['ARRIVEDEPART'] == 1:
    row['IMBAL_PORT_FACT_PV'] = 0.94
elif row['IMBAL_PORT_GRP_PV'] == 12 and row['ARRIVEDEPART'] == 2:
    row['IMBAL_PORT_FACT_PV'] = 1.06
elif row['IMBAL_PORT_GRP_PV'] == 13 and row['ARRIVEDEPART'] == 1:
    row['IMBAL_PORT_FACT_PV'] = 0.97
elif row['IMBAL_PORT_GRP_PV'] == 13 and row['ARRIVEDEPART'] == 2:
    row['IMBAL_PORT_FACT_PV'] = 1.03
elif row['IMBAL_PORT_GRP_PV'] == 14 and row['ARRIVEDEPART'] == 1:
    row['IMBAL_PORT_FACT_PV'] = 0.97
elif row['IMBAL_PORT_GRP_PV'] == 14 and row['ARRIVEDEPART'] == 2:
    row['IMBAL_PORT_FACT_PV'] = 1.03
elif row['IMBAL_PORT_GRP_PV'] == 15 and row['ARRIVEDEPART'] == 1:
    row['IMBAL_PORT_FACT_PV'] = 0.95
elif row['IMBAL_PORT_GRP_PV'] == 15 and row['ARRIVEDEPART'] == 2:
    row['IMBAL_PORT_FACT_PV'] = 1.03
elif row['IMBAL_PORT_GRP_PV'] == 16 and row['ARRIVEDEPART'] == 1:
    row['IMBAL_PORT_FACT_PV'] = 0.95
elif row['IMBAL_PORT_GRP_PV'] == 16 and row['ARRIVEDEPART'] == 2:
    row['IMBAL_PORT_FACT_PV'] = 1.03
elif row['IMBAL_PORT_GRP_PV'] == 17 and row['ARRIVEDEPART'] == 1:
    row['IMBAL_PORT_FACT_PV'] = 0.97
elif row['IMBAL_PORT_GRP_PV'] == 17 and row['ARRIVEDEPART'] == 2:
    row['IMBAL_PORT_FACT_PV'] = 1.03
else:
    row['IMBAL_PORT_FACT_PV'] = 1.0"""