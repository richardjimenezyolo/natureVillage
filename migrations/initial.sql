CREATE TABLE `houses_raw` (
  `id` bigint NOT NULL AUTO_INCREMENT,
  `topic` varchar(255) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `message` varchar(255) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `created_at` datetime DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB AUTO_INCREMENT=533 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE VIEW house as with `cte` as (
select
    regexp_replace(`hr`.`topic`, 'Nature_Village/HOUSES/', '') AS `topic`,
    `hr`.`message` AS `message`,
    `hr`.`created_at` AS `created_at`
from
    `houses_raw` `hr`),
`parsed` as (
select
    regexp_replace(`cte`.`topic`, '/.*$', '') AS `house`,
    regexp_replace(`cte`.`topic`, '.*/', '') AS `metric`,
    `cte`.`message` AS `message`,
    `cte`.`created_at` AS `created_at`
from
    `cte`),
`parsed_data_value` as (
select
    `parsed`.`house` AS `house`,
    `parsed`.`metric` AS `metric`,
    `parsed`.`message` AS `message`,
    `parsed`.`created_at` AS `created_at`,
    lead(`parsed`.`message`, 1) OVER (
    ORDER BY `parsed`.`metric` ) AS `value`
from
    `parsed`
where
    ((1 = 1)
        and (`parsed`.`metric` = 'data'))),
`joined_parse` as (
select
    `parsed_data_value`.`house` AS `house`,
    `parsed_data_value`.`metric` AS `metric`,
    `parsed_data_value`.`message` AS `message`,
    `parsed_data_value`.`created_at` AS `created_at`,
    `parsed_data_value`.`value` AS `value`
from
    `parsed_data_value`
where
    (`parsed_data_value`.`message` like '>%')
union all
select
    `parsed`.`house` AS `house`,
    `parsed`.`metric` AS `metric`,
    `parsed`.`message` AS `message`,
    `parsed`.`created_at` AS `created_at`,
    `parsed`.`message` AS `message`
from
    `parsed`
where
    (`parsed`.`metric` <> 'data'))
select
ROW_NUMBER() over (PARTITION by house)as id,
    `joined_parse`.`house` AS `house`,
    max(if((`joined_parse`.`metric` = 'WATER_METER'), `joined_parse`.`message`, 0)) AS `water_gal`,
    max(if((`joined_parse`.`metric` = 'ENERGY_METER'), `joined_parse`.`message`, 0)) AS `energy_wh`,
    max(if(((`joined_parse`.`message` like '%wi%fi%') and regexp_like(`joined_parse`.`value`, '[0-9]]*')), `joined_parse`.`value`, NULL)) AS `wifi_dbm`,
    max(if((`joined_parse`.`message` like '%electrovalvula%' and `joined_parse`.`value` not REGEXP '[0-9]' ), `joined_parse`.`value`, NULL)) AS `electrovalvula`,
    max(if((`joined_parse`.`message` like '%contactor%' and `joined_parse`.`value` not REGEXP '[0-9]'), `joined_parse`.`value`, NULL)) AS `contactor`,
    max(`joined_parse`.`created_at`) AS `emitted_at`
from
    `joined_parse`
group by
    `joined_parse`.`house`,
    cast(`joined_parse`.`created_at` as date),
    minute(`joined_parse`.`created_at`)
order by
    `emitted_at`;


CREATE TABLE `naturevillage_houses` (
  `id` bigint NOT NULL AUTO_INCREMENT,
  `house` varchar(255) DEFAULT NULL,
  `water_gal` int unsigned DEFAULT NULL,
  `energy_wh` int unsigned DEFAULT NULL,
  `wifi_dbm` varchar(10) DEFAULT NULL,
  `electrovalvula` varchar(255) DEFAULT NULL,
  `contactor` varchar(255) DEFAULT NULL,
  `emitted_at` datetime DEFAULT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB AUTO_INCREMENT=4423 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

CREATE EVENT `sync_houses_table` 
ON SCHEDULE EVERY 1 MINUTE  
 DO insert into naturevillage_houses (house, water_gal, energy_wh, wifi_dbm, electrovalvula, contactor, emitted_at)
   select house, water_gal, energy_wh, wifi_dbm, electrovalvula, contactor, emitted_at 
   from house
   where emitted_at > (SELECT emitted_at from naturevillage_houses order by emitted_at desc limit 1);

