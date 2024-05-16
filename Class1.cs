https://www.timeapi.io/api/TimeZone/zone?timeZone=Europe/Amsterdam

TimeZoneData{
    timeZone    string
    nullable: true
currentLocalTime    string($date - time)
currentUtcOffset Offset{
        seconds integer($int32)
readOnly: true
milliseconds integer($int32)
readOnly: true
ticks integer($int64)
readOnly: true
nanoseconds integer($int64)
readOnly: true
}
    standardUtcOffset Offset{
        seconds integer($int32)
    readOnly: true
    milliseconds integer($int32)
    readOnly: true
    ticks integer($int64)
    readOnly: true
    nanoseconds integer($int64)
    readOnly: true
    }
    hasDayLightSaving boolean
isDayLightSavingActive boolean
dstInterval DstInterval{
        dstName string
        nullable: true
dstOffsetToUtc Offset{ ...}
        dstOffsetToStandardTime Offset{ ...}
        dstStart    string($date - time)
nullable: true
dstEnd  string($date - time)
nullable: true
dstDuration Duration{ ...}
    }
}