#this program subtracts 2 dates of the type HH:MM:SS

{
    hh1=substr($1,1,2);
    mm1=substr($1,4,2);
    ss1=substr($1,7,2);

    hh2=substr($2,1,2);
    mm2=substr($2,4,2);
    ss2=substr($2,7,2);

    time1=hh1*60*60 + mm1*60 + ss1;
    time2=hh2*60*60 + mm2*60 + ss2;

    diff=time2-time1;

    #printf "%s %d:%d:%d\n",$1,diff/(60*60),diff%(60*60)/60,diff%60;
    printf "%d\n",diff;
}
