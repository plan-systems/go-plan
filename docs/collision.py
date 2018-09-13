#!/usr/bin/python

# Inspired by http://preshing.com/20110504/hash-collision-probabilities/

import math

N = 2^32
probUnique = 1.0

oddsList = [
    2,
    100,
    1000,
    10000,
    100000,
    1000000,
    10000000,
    100000000,
    1000000000,     # 1 billion
    1e10,
    1e11,
    1e12,
    1e13,
    1e14,
    1e15,
    1e16,
    1e17,
    1e18,
    1e19,
]

bitList = [
    32,
    64,
    128,
    160,
    192,
    256,
    512,
]


def getPrettyInt(N):
    fN = float(N)
    if fN < 1000000.:
        s = "%d" % int(fN)
    elif fN < 1e9:
        s = "%.1f million" % math.floor(fN/1e6)
    elif fN < 1e12:
        s = "%.1f billion" % (fN/1e9)
    elif fN < 1e15:
        s = "%.1f trillion" % (fN/1e12)
    else:
        s = "%.1e" % fN

    s = s.replace( ".0", "", 1 )
    s = s.replace( " 1 ", " a " )

    return "%-17s" % s


if 1:
    line = "%-22s" % "Collision Odds"
    for numBits in bitList:
        line += "%-17s" % ("%d bytes" % (numBits/8))
    print line

for odds in oddsList:
    line = '1 in ' + getPrettyInt(odds)
    Pcollision = 1./float(odds)
    for numBits in bitList:
        N = float(2**numBits)
        numPicksNeeded = math.ceil( math.sqrt(2. * N * Pcollision) )
        line += getPrettyInt(numPicksNeeded)
    print line




