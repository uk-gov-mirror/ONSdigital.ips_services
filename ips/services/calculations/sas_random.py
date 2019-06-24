from mpmath import mp


class SASRandom():

    seedlist = []

    def __init__(self, seed):
        mp.dps = 17
        self.seedlist = [seed]

    def random(self):
        m = mp.mpf(2 ** 31 - 1)
        a = mp.mpf(397204094)
        inputseed = self.seedlist[-1]

        if inputseed < 1:
            inputseed = inputseed * m

        outputseed = inputseed * a % m

        self.seedlist.append(outputseed)

        return outputseed / m
