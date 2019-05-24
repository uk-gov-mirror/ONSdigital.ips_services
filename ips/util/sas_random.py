from mpmath import *
mp.dps = 17


def seed(inputseed):
    global seedlist
    seedlist = [inputseed]
    global prev
    prev = 0


def sas_random(x):
    global prev
    # print(prev, x)
    if prev != x:
        m = mp.mpf(2 ** 31 - 1)
        a = mp.mpf(397204094)
        inputseed = seedlist[-1]

        if inputseed < 1:
            inputseed = inputseed * m

        outputseed = inputseed * a % m

        seedlist.append(outputseed)
        prev = x
        global output
        output = outputseed / m
        # if len(seedlist) < 50:
        #     print(x, output)
        return output
    else:
        # print(x)
        return output






