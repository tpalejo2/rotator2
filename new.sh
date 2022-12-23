#!bin/bash
dbUser=deliveryCore
dbPass=PW65zu2uaMZpTcNE
database=DeliveryCore
masterServer=192.168.100.123
slaveServer=192.168.100.229

#Folder to download the tables data
local_backup_path=/backups/new_rotator

#Folder to storage data / GCP
bucket_gcp=public-aldeamo
output_cloud_folder=day_sms_data

#Room to send notifications
room_url="https://chat.googleapis.com/v1/spaces/AAAApdUZgbY/messages?key=AIzaSyDdI0hCZtE6vySjMm-WEfRq3CPzqKqqsHI&token=_kOtyxbzZD8vtVKqyIaQ9tUaghWcCtDvNx7TraVGCCk%3D⬢
?
⬢
?
⬢
?
⬢
?
⬢
?
⬢
?
⬢
?
⬢
?
"
#room_url="https://chat.googleapis.com/v1/spaces/AAAAw8MP2Io/messages?key=AIzaSyDdI0hCZtE6vySjMm-WEfRq3CPzqKqqsHI&token=t7gD_SFwZ_NqrHdg2ZvUkFNQM6FXih2pT8315Vg0G_U%3D⬢
?
⬢
?
⬢
?
⬢
?
⬢
?
⬢
?
⬢
?
⬢
?
"
#New rotator ouput to history
file_lines=1000000
history_db_ip=192.168.100.124

function execute_rotation () {
        local nowEx=$(date +"%Y-%m-%d %H:%M:%S")
        local logInt="$5[tExec=$nowEx]"
        local int_land=$1
        local str_tableName=$2
        local str_where=$3
        local str_queryIndex=$4
        local countRowsToMove=0
        local countRowsMoved=0
        local bolSuccessMoved=0
        local dayObj=$6
        local expected_rows=$9
        local countRowsToMove=$10
        echo "####"
        echo "##########Executing rotation for Min:${dateMin} - Max: ${dateMax} / Tablename: ${str_tableName}"
        echo "####"
        logInt+="[tableName=$str_tableName]"        
        countRowsToMove=`mysql -u$dbUser -p$dbPass -D$database -h$masterServer -s -N -e "SELECT COUNT(1) FROM DeliverySms${int_land} FORCE INDEX ($str_queryIndex) WHERE dateToSend BETWEEN '${dateMin}' AND '${dateMax}' AND ${str_where};"`
        logInt+="[RowsToMove=$countRowsToMove]"

        if [ $countRowsToMove -gt 0 ]
        then
                local start=`date +%s`
                local backupFilename="${folder}/${str_tableName}-${dayObj}.sql"
               
                #Lock tables
                echo "LOCK TABLE $str_tableName WRITE, DeliverySms${land} READ, ConfigRotateToHistory READ;" | `mysql -h$slaveServer -u$dbUser -p$dbPass -D$database`
               
                echo "Main expected rows ${expected_rows}"
                generate_backup "${land}" "${str_where}" "${backupFilename}" "${str_queryIndex}" "$expected_rows" "$dateMin" "$dateMax" "$countRowsToMove"
               
                #Unlock Tables
                echo "UNLOCK TABLES;" | `mysql -h$slaveServer -u$dbUser -p$dbPass -D$database`

                if [ $str_queryIndex == "idx_rep_ownSend" ] # Para ajustar el indice y que quede de manera adecuada para la tabla historica
                then
                        str_queryIndex="${str_queryIndex}Hist"
                fi

                #Verify saved rows
                savedRows=`wc -l $backupFilename | awk {'print $1'}`
                logInt+="[SavedRows=$savedRows]"

                if [ $savedRows = $countRowsToMove ]
                then
                        logInt+="[SuccessMove]"
                        bolSuccessMoved=1
                        echo "Data has been copied successfuly :D"
                else
                        logInt+="[ErrorMove]"
                        bolSuccessMoved=2
                        local message='\n*ERROR - La consistencia de los datos no se pudo garantizar para la tabla'${tablename}'*\n\n'${logInt}
                        send_room_message "${message}"
                        echo "we should break whole process and send a critical message"
                fi
       
                local end=`date +%s`
                local timeMove=$((end-start))
                logInt+="[timeMove=$timeMove]"
        else
                logInt+="[NoRowsToMove]"
                bolSuccessMoved=3
                local message='\n*ADVERTENCIA - No hay datos para mover a la tabla '${str_tableName}'*\n\n'
                message+=${logInt}
                send_room_message "${message}"
        fi

        #Inserto en tabla de control
        local idControlRotate=$(echo "INSERT INTO ControlRotateToHistory_new (country, tableName, date, status, rowsMoved, log) VALUES ($int_land,'$str_tableName','$nowEx',$bolSuccessMoved,$countRowsMoved,'$logInt');SELECT LAST_INSERT_ID();" | mysql -u$dbUser -p$dbPass -h$slaveServer -D$database -s -N)

        if [ $bolSuccessMoved -eq 1 ] && [ $queryIndex = "idx_dateToSend" ]; # last item - regular clients
        then
                local output_folder="${output_cloud_folder}/${dayObj}/${land}"
                local input_folder=$folder
                upload_backup $input_folder $output_folder
                echo "Files have been uploaded"

                local consistency_result=`verify_backup_consistency ${savedRows} ${input_folder} ${output_folder}`
                echo "Consistency result ${consistency_result}"
                if [ $consistency_result == "ok" ]
                then
                    echo "Table ${str_tableName} has been upload successfully"
                   
                   
                    if [ $load = "history" ]
                    then
                        echo "It's going to load data in History"
                        sh /home/celuman/bin/load_data_to_history_threads.sh ${folder}
                        local rows_in_history
                        #local rows_in_history=`mysql -u$dbUser -p$dbPass -D$database -h$history_db_ip -s -N -e "SELECT COUNT(1) FROM ${str_tableName} FORCE INDEX (idx_dateToSend) WHERE dateToSend BETWEEN '$dateMin' AND '$dateMax';"`
# -----------------------------------------------------------------------------------------------------------------
# LD --> Suma de los totales por tabla para colombia y peru, para que la comparación del siguiente if no salte al error a pesar de rotar bien la data.
rows_in_history=0
if [ $int_land=='57' ] || [ $int_land=='51' ]
                        then
                            for tabla in $(ls /backups/new_rotator/$dayObj/$int_land/DeliverySms* | awk -F"/" '{print $6}' | awk -F"-" '{print $1}')
                            do
echo ">> tabla $tabla"
                                local temp=`mysql -u$dbUser -p$dbPass -D$database -h$history_db_ip -s -N -e "SELECT COUNT(1) FROM ${tabla} FORCE INDEX (idx_dateToSend) WHERE dateToSend BETWEEN '$dateMin' AND '$dateMax';"`
                                rows_in_history=$(expr $temp + $rows_in_history)
echo ">> rows_in_history en $tabla = $rows_in_history"
                            done
                        else
                        local rows_in_history=`mysql -u$dbUser -p$dbPass -D$database -h$history_db_ip -s -N -e "SELECT COUNT(1) FROM ${str_tableName} FORCE INDEX (idx_dateToSend) WHERE dateToSend BETWEEN '$dateMin' AND '$dateMax';"`
echo ">>rows_in_history en ${str_tableName}  = $rows_in_history"
                        fi

                        echo ">> rows_in_history para $int_land es $rows_in_history"
# -----------------------------------------------------------------------------------------------------------------

if [ $rows_in_history != $expected_rows ] #-8
                        then
local message='\n*An Error has occurred trying to copy the day records to history in '${str_tableName}'*\n\n'
message+='Expected rows: *'$expected_rows'*'
                                        message+='\nSaved rows: *'$savedRows'*'
                                        message+='\nRows in history: *'$rows_in_history'*'
                                        send_room_message "${message}"
                         else
        echo "Ejecutando borrado con script sh /home/celuman/bin/delete-day.sh $dateMin $int_land 1"
                            sh /home/celuman/bin/delete-day.sh "$dateMin" $int_land 1;
                            #deleteData=`mysql -u$dbUser -p$dbPass -D$database -h$masterServer -s -N -e "DELETE FROM DeliverySms${int_land} WHERE $str_where;"`  
                            #echo "Query ejecutada ${deleteData}"
           
                                local message='\n*Finished DELETE for table DeliverySms'${land}'*\n\n'
               send_room_message "${message}"
                           
                                rows_in_actual=`mysql -u$dbUser -p$dbPass -D$database -h$masterServer -s -N -e "SELECT COUNT(1) FROM DeliverySms${land} FORCE INDEX (idx_dateToSend) WHERE dateToSend BETWEEN '$dateMin' AND '$dateMax';"`

                                if [ $rows_in_actual == 0 ] #-9
                                then
                                optimizeTable=`mysql -u$dbUser -p$dbPass -D$masterServer -h192.168.200.23 -s -N -e "OPTIMIZE TABLE DeliverySms${land};"`
               local message='\n*Finished OPTIMIZE for table. DeliverySms'${land}'*\n\n'
                                send_room_message "${message}"
                            fi #-9
                        fi #-8
                    fi #-6

                    #rm -rf ${folder}
                   
                    echo "It should delete records and optimize table ***"
                    mysql -u$dbUser -p$dbPass -D$database -e "UPDATE ControlRotateToHistory_new SET status=4, log='$logInt' WHERE id=$idControlRotate;"
               
                else
                    local message='\n*ERROR en rotación DeliverySms'${land}'  Datos no consistentes*\n'
                    message+='Ocurrio un error moviendo la informacion a '${str_tableName}'. Detalle del log es: '${consistency_result}
                    send_room_message "${message}"
                    echo "We should execute a roleback :( ******"    
                fi
        fi

        echo $logInt
        dateLog=$(date +"%Y-%m")
        pathLog="/home/celuman/logs/DeliverySms/NewRotator_${dateLog}.log"
        now=$(date +"%Y-%m-%d %H:%M:%S")
        echo "$now -> [land=${land}]$logInt" >> ${pathLog} 2>&1
}

function generate_backup() {
    #Generate backup
    local land=$1
    local whereSource=$2
    local backupFilename=$3
    local str_queryIndex=$4
    local expected_rows=$5
    local dateMin=$6
    local dateMax=$7
    if [ $expected_rows -ge 5000000 ]
    then
        local dmi=$(date -d "${dateMin}" +"%Y-%m-%d 00:00:00")
        local plus_time=$(date -d "${dmi} 8 hours" +"%Y-%m-%d %H:%M:%S")
        local dmx=$(date -d "${plus_time} 1 sec ago" +"%Y-%m-%d %H:%M:%S.999")

        local querySql="SELECT gsm, replace(replace(message,CHAR(13),''),CHAR(10),''), processStatus, messageFormat, priority, dateIn, dateToSend, dateUpdated, source, totalMessages, correlationLabel, transactionId, externalId, Service_id, ShortCode_id, shortCode, metadata, Connection_id, Operator_Id, ownerId FROM DeliverySms${land} FORCE INDEX ($str_queryIndex)"
        local ws=" WHERE dateToSend BETWEEN '${dmi}' AND '${dmx}' AND ${whereSource}"
       
        echo "${querySql} ${ws};"
        mysql -h$slaveServer -u$dbUser -p$dbPass -D$database --skip-column-names  -e "${querySql} ${ws};"  > "${backupFilename}-01"

        dmi=$(date -d "${dmx} 1 sec" +"%Y-%m-%d %H:%M:%S.000")
        plus_time=$(date -d "${dmi} 2 hours" +"%Y-%m-%d %H:%M:%S")
        dmx=$(date -d "${plus_time} 1 sec ago" +"%Y-%m-%d %H:%M:%S.999")
        ws=" WHERE dateToSend BETWEEN '${dmi}' AND '${dmx}' AND ${whereSource}"
        echo "${querySql} ${ws};"
        mysql -h$slaveServer -u$dbUser -p$dbPass -D$database --skip-column-names  -e "${querySql} ${ws};"  > "${backupFilename}-02"

        dmi=$(date -d "${dmx} 1 sec" +"%Y-%m-%d %H:%M:%S.000")
        plus_time=$(date -d "${dmi} 2 hours" +"%Y-%m-%d %H:%M:%S")
        dmx=$(date -d "${plus_time} 1 sec ago" +"%Y-%m-%d %H:%M:%S.999")
        ws=" WHERE dateToSend BETWEEN '${dmi}' AND '${dmx}' AND ${whereSource}"
        echo "${querySql} ${ws};"
        mysql -h$slaveServer -u$dbUser -p$dbPass -D$database --skip-column-names  -e "${querySql} ${ws};"  > "${backupFilename}-03"

        dmi=$(date -d "${dmx} 1 sec" +"%Y-%m-%d %H:%M:%S.000")
        plus_time=$(date -d "${dmi} 2 hours" +"%Y-%m-%d %H:%M:%S")
        dmx=$(date -d "${plus_time} 1 sec ago" +"%Y-%m-%d %H:%M:%S.999")
        ws=" WHERE dateToSend BETWEEN '${dmi}' AND '${dmx}' AND ${whereSource}"
        echo "${querySql} ${ws};"
        mysql -h$slaveServer -u$dbUser -p$dbPass -D$database --skip-column-names  -e "${querySql} ${ws};"  > "${backupFilename}-04"

        dmi=$(date -d "${dmx} 1 sec" +"%Y-%m-%d %H:%M:%S.000")
        plus_time=$(date -d "${dmi} 2 hours" +"%Y-%m-%d %H:%M:%S")
        dmx=$(date -d "${plus_time} 1 sec ago" +"%Y-%m-%d %H:%M:%S.999")
        ws=" WHERE dateToSend BETWEEN '${dmi}' AND '${dmx}' AND ${whereSource}"
        echo "${querySql} ${ws};"
        mysql -h$slaveServer -u$dbUser -p$dbPass -D$database --skip-column-names  -e "${querySql} ${ws};"  > "${backupFilename}-05"

        dmi=$(date -d "${dmx} 1 sec" +"%Y-%m-%d %H:%M:%S.000")
        plus_time=$(date -d "${dmi} 2 hours" +"%Y-%m-%d %H:%M:%S")
        dmx=$(date -d "${plus_time} 1 sec ago" +"%Y-%m-%d %H:%M:%S.999")
        ws=" WHERE dateToSend BETWEEN '${dmi}' AND '${dmx}' AND ${whereSource}"
        echo "${querySql} ${ws};"
        mysql -h$slaveServer -u$dbUser -p$dbPass -D$database --skip-column-names  -e "${querySql} ${ws};"  > "${backupFilename}-06"

        dmi=$(date -d "${dmx} 1 sec" +"%Y-%m-%d %H:%M:%S.000")
        plus_time=$(date -d "${dmi} 2 hours" +"%Y-%m-%d %H:%M:%S")
        dmx=$(date -d "${plus_time} 1 sec ago" +"%Y-%m-%d %H:%M:%S.999")
        ws=" WHERE dateToSend BETWEEN '${dmi}' AND '${dmx}' AND ${whereSource}"
        echo "${querySql} ${ws};"
        mysql -h$slaveServer -u$dbUser -p$dbPass -D$database --skip-column-names  -e "${querySql} ${ws};"  > "${backupFilename}-07"

        dmi=$(date -d "${dmx} 1 sec" +"%Y-%m-%d %H:%M:%S.000")
        plus_time=$(date -d "${dmi} 4 hours" +"%Y-%m-%d %H:%M:%S")
        dmx=$(date -d "${plus_time} 1 sec ago" +"%Y-%m-%d %H:%M:%S.999")
        ws=" WHERE dateToSend BETWEEN '${dmi}' AND '${dmx}' AND ${whereSource}"
        echo "${querySql} ${ws};"
        mysql -h$slaveServer -u$dbUser -p$dbPass -D$database --skip-column-names  -e "${querySql} ${ws};"  > "${backupFilename}-08"

        cat ${backupFilename}-* > "${backupFilename}"
        rm -rf ${backupFilename}-*
    else
        whereSource=" WHERE dateToSend BETWEEN '${dateMin}' AND '${dateMax}' AND ${whereSource}"
        local querySql="SELECT gsm, replace(replace(message,CHAR(13),''),CHAR(10),''), processStatus, messageFormat, priority, dateIn, dateToSend, dateUpdated, source, totalMessages, correlationLabel, transactionId, externalId, Service_id, ShortCode_id, shortCode, metadata, Connection_id, Operator_Id, ownerId FROM DeliverySms${land} FORCE INDEX ($str_queryIndex) $whereSource;"
        mysql -h$slaveServer -u$dbUser -p$dbPass -D$database --skip-column-names  -e "${querySql}"  > "${backupFilename}"
    fi          
}
function replicationValidate(){
        local timeRep=0
        timeRep=`mysql --login-path=zabbix_monitor -e "show slave status\G" | grep -i Seconds_Behind_Master |awk {'print $2'}`
        echo $timeRep
}

function get_indepent_clients() {
        local -a results=()
        local tn=""
        mysql -u$dbUser -p$dbPass -D$database -NBe "SELECT DISTINCT(tableName) AS tableName FROM ConfigRotateToHistory WHERE land=$land AND status=1"  | while IFS='@' read -r tn;
        do
            echo "${tn}"
        done
}

function send_room_message() {
    local message=$1
    local room_url=$room_url
    local body='{"text":"'${message}'"}'
    curl -X POST -H 'Content-Type: application/json' -d "${body}" "${room_url}" >/dev/null
}

function upload_backup() {
    local source=$1
    local output=$2
    gsutil cp -r ${source}/* "gs://${bucket_gcp}/${output}"
}

function verify_backup_consistency() {
    local saved_rows=$1
    local input_folder=$2
    local output_folder=$3
    local i_size=`ls -l ${input_folder} |  awk -F' ' '{sum+=$5;} END{print sum;}'`
    local o_size=`gsutil du -s -a gs://${bucket_gcp}/${output_folder} |  awk '{printf $1}'`
    if [ $i_size = $o_size ]
    then
        echo "ok"
    else
        echo "failed - Input: ${i_size}  /  Ouput: ${o_size}"
    fi
}

function load_data_from_file() {
    echo "####### Loading data from ${f_} #######"
    local filename=$1
    local root_folder=$2
    local output_file=$3
    local total_records=`wc -l $filename | awk '{print $1}'`
    local saved_records=0
    local tableName=`echo $filename | awk -F'-' '{print $1}'`
    split -l $file_lines --numeric-suffixes=1 --additional-suffix=$1 --elide-empty-files $1
    for f_ in *$filename ;
    do
        if [ "$f_" != "$filename" ]
        then
            local records=`wc -l $f_ | awk '{print $1}'`
            local query="LOAD DATA LOCAL INFILE '$root_folder/$f_' INTO TABLE DeliveryCore.${tableName} (gsm, message, processStatus, messageFormat, priority, dateIn, dateToSend, dateUpdated, source, totalMessages, correlationLabel, transactionId, externalId, Service_id, ShortCode_id, shortCode, metadata, Connection_id, Operator_Id, ownerId);"
            echo "${query}"
            mysql -h$history_db_ip -u$dbUser -p$dbPass -D$database -e "${query}"
            saved_records=$(( saved_records + records ))
            rm -rf $f_
        fi
    done

    if [ $total_records = $saved_records ]
    then
        echo "Load data from file ${saved_records} ${loaded_records}"
        echo "${saved_records} ${tableName} $(date +"%d-%m %H:%M:%S")" >> $output_file
    else
        echo "Error -1 / Saved records: ${saved_records} / Expected records: ${total_records} " >> $output_file
    fi
}

function load_data_from_folder() {
    local folder=$1
    local output_file=$folder/output.txt
    touch $output_file
    local total_records=`cat rows`
    local success_records=0
    local command=""
    local i=0
    for f_ in $folder/*.sql ;
    do
      if [ "$f_" != "rows" ];
        then
            load_data_from_file ${f_} $folder $output_file &
            i=$(( i + 1 ))
        fi
    done

    local scan_records=`wc -l $output_file | awk '{print $1}'`
    while [ "${scan_records}" != "${i}" ]
    do
        sleep 20
        scan_records=`wc -l $output_file | awk '{print $1}'`
    done

    local loaded_records=`cat $output_file | awk -F' ' '{sum+=$1;} END{print sum;}'`
    if [ $loaded_records != $total_records ]
    then
        echo "failed"
    else
        echo "ok"
        rm -rf $output_file
    fi
}
###################################################################
########################## Inicio script ##########################
###################################################################

land=$1
load=""
if [ -z $3 ]
then
    load="local"
else
    load=$3
fi

dayToExec=`date -d $2 +"%Y-%m-%d 00:00:00"`
log=""

timeReplication=0
#$(replicationValidate)
max_replication_delay=600

#Calcúlo los segundos respecto al inicio del dia
int_hoursInSeconds=$((`date +"10"` * 3600))
int_hoursInSeconds=9999
#int_minutesInSeconds=$((`date +"%M"` * 60))
int_seconds=$((int_hoursInSeconds + int_minutesInSeconds))

log+="[secondsCurdate=$int_seconds]"

if [ $timeReplication == "NULL" ]
then
        log+="[ReplicationDown]"
        bolSuccessMoved=5
        idControlRotate=$(echo "INSERT INTO ControlRotateToHistory_new (country, tableName, date, status, rowsMoved, log) VALUES ($land,'DeliverySms${land}History','$now',$bolSuccessMoved,0,'$log');SELECT LAST_INSERT_ID();" | mysql -u$dbUser -p$dbPass -D$database -s -N)
        message='\n*ERROR en rotación DeliverySms'${land}'History - ReplicationDown*\n'
        message+='Ocurrio un error moviendo la informacion de DeliverySms'${land}' a DeliverySms'${land}'History. Detalle del log es: '${log}
        send_room_message "${message}"
fi
       
if [ $timeReplication -gt $max_replication_delay ]
then
        log+="[ReplicationTracked][timeReplication=$timeReplication]"
        bolSuccessMoved=5
        idControlRotate=$(echo "INSERT INTO ControlRotateToHistory_new (country, tableName, date, status, rowsMoved, log) VALUES ($land,'DeliverySms${land}History','$now',$bolSuccessMoved,0,'$log');SELECT LAST_INSERT_ID();" | mysql -u$dbUser -p$dbPass -D$database -s -N)
        message='\n*ERROR en rotación DeliverySms'${land}'History - ReplicationDalay is bigger than expected*\n'
        message+='Ocurrio un error moviendo la informacion de DeliverySms'${land}' a DeliverySms'${land}'History. Detalle del log es: '{$log}
        send_room_message "${message}"
fi

if [ $max_replication_delay -gt $timeReplication ]
then
        log+="[timeReplication=$timeReplication]"

        declare -A arr_timeDif=( [57]=0 [51]=0 [593]=0 [52]=0 [507]=0 [502]=-1 [503]=-1 [504]=-1 [505]=-1 [506]=-1 [58]=1 [591]=1 [1]=1 [54]=2 [56]=2 [595]=2 [598]=2 [55]=3 [34]=6 [351]=5 )

        timeDif=${arr_timeDif[$land]}

        if [ $timeDif -ge 0 ]
        then
                dateMin=$(date -d "$dayToExec $timeDif hour ago" +"%Y-%m-%d %H:%M:%S")
        else
                timeDif=${timeDif#-} #calculo el valor absoluto
                dateMin=$(date -d "$dayToExec $timeDif hour" +"%Y-%m-%d %H:%M:%S")
        fi

        next_day=$(date -d "$dateMin 1 day" +"%Y-%m-%d %H:%M:%S")
        dateMax=$(date -d "$next_day 1 sec ago" +"%Y-%m-%d %H:%M:%S.999")
        dayObj=$(date -d "$dayToExec" +"%Y-%m-%d")
        echo "Running backup for country ${land} / MinDate: ${dateMin} / MaxDate: ${dateMax}"

        folder="${local_backup_path}/${dayObj}/${land}"
        mkdir -p $folder
        expected_rows=`mysql -u$dbUser -p$dbPass -D$database -h$masterServer -s -N -e "SELECT COUNT(1) FROM DeliverySms${land} FORCE INDEX (idx_dateToSend) WHERE dateToSend BETWEEN '$dateMin' AND '$dateMax';"`
        echo $expected_rows > "${folder}/rows"

        log+="[tMin=$dateMin][tMax=$dateMax]"
       
        tableNames=( $( get_indepent_clients "$dbUser" "$dbPass" "$database" "$land" ) )
        tablesToIgnore=""
        for tableName in "${tableNames[@]}"
        do
                logTemp=$log
                ############# Control para cuando se retrasa la replicacion ################
                timeReplication=0
                #$(replicationValidate)
                controlDelay=1
                logTemp+="[replicationBeforeProcess=$timeReplication]"                
                while [ $timeReplication -ge 10 ] && [ $controlDelay -le 10 ]
                do
                        timeSleep=$(( $controlDelay * 60 ))
                        sleep $timeSleep
                        timeReplication=0
                        #$(replicationValidate)
                        logTemp+="[replicationAfterSleep_$controlDelay=$timeReplication]"
                        controlDelay=$(( $controlDelay + 1 ))
               done
                ############################################################################

               
                whereSource="source IN (SELECT Client_id FROM ConfigRotateToHistory WHERE tableName='$tableName' AND status=1)"
                queryIndex="idx_rep_ownSend"
                echo "Table to process ${tableName} *****************************************************"
                execute_rotation $land $tableName "$whereSource" $queryIndex "$logTemp" "$dayObj" "$dateMax" "$dateMin" $expected_rows
        done
       
        logTemp=$log

        ######Charlie Note: I think this code could be unnecessary {
        if [ $land -eq 51 ] || [ $land -eq 57 ]
        then
                ############# Control para cuando se retrasa la replicacion ################
                timeReplication=0
                #$(replicationValidate)
                controlDelay=1
                logTemp+="[replicationBeforeProcess=$timeReplication]"
                               
                while [ $timeReplication -ge 10 ] && [ $controlDelay -le 10 ]
                do
                        timeSleep=$(( $controlDelay * 60 ))
                        sleep $timeSleep
                        timeReplication=0
                        #$(replicationValidate)
                        logTemp+="[replicationAfterSleep_$controlDelay=$timeReplication]"
                        controlDelay=$(( $controlDelay + 1 ))
                done
        fi
        ###### } Charlie Note: I think this code could be unnecessary
 
        ##############  Se rota la tabla principal ########################
        whereSource=" source NOT IN (SELECT Client_id FROM ConfigRotateToHistory WHERE land='${land}' AND status=1) "
        queryIndex="idx_dateToSend"
        execute_rotation $land "DeliverySms${land}History" "$whereSource" $queryIndex "$logTemp" "$dayObj" "$dateMax" "$dateMin" $expected_rows
fi
