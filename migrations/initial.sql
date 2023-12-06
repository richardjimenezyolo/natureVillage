CREATE TABLE `houses_raw` (
  `id` bigint NOT NULL AUTO_INCREMENT,
  `topic` varchar(255) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `message` varchar(255) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `created_at` datetime DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB AUTO_INCREMENT=533 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE OR REPLACE ALGORITHM = UNDEFINED VIEW `house` AS with cte as (
select
    regexp_replace(`hr`.`topic`, 'Nature_Village/HOUSES/', '') AS `topic`,
    `hr`.`message` AS `message`,
    `hr`.`created_at` AS `created_at`
from
    `houses_raw` `hr`),
parsed as (
select
    regexp_replace(`cte`.`topic`, '/.*$', '') AS `house`,
    regexp_replace(`cte`.`topic`, '.*/', '') AS `metric`,
    `cte`.`message` AS `message`,
    `cte`.`created_at` AS `created_at`
from
    `cte`),
parsed_data_value as (
select
    `parsed`.`house` AS `house`,
    `parsed`.`metric` AS `metric`,
    `parsed`.`message` AS `message`,
    `parsed`.`created_at` AS `created_at`,
    lead(`parsed`.`message`, 1) over (
order by
    `parsed`.`metric`) AS `value`
from
    `parsed`
where
    1 = 1
    and `parsed`.`metric` = 'data'),
joined_parse as (
select
    `parsed_data_value`.`house` AS `house`,
    `parsed_data_value`.`metric` AS `metric`,
    `parsed_data_value`.`message` AS `message`,
    `parsed_data_value`.`created_at` AS `created_at`,
    `parsed_data_value`.`value` AS `value`
from
    `parsed_data_value`
where
    `parsed_data_value`.`message` like '>%'
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
    `parsed`.`metric` <> 'data'
)select
    `joined_parse`.`house` AS `house`,
    max(if(`joined_parse`.`metric` = 'WATER_METER', `joined_parse`.`message`, 0)) AS `water_gal`,
    max(if(`joined_parse`.`metric` = 'ENERGY_METER', `joined_parse`.`message`, 0)) AS `energy_wh`,
    max(if(`joined_parse`.`message` like '%wi%fi%' and `joined_parse`.`value` regexp '[0-9]]*', `joined_parse`.`value`, NULL)) AS `wifi_dbm`,
    max(if(`joined_parse`.`message` like '%electrovalvula%', `joined_parse`.`value`, NULL)) AS `electrovalvula`,
    max(if(`joined_parse`.`message` like '%contactor%', `joined_parse`.`value`, NULL)) AS `contactor`,
    max(`joined_parse`.`created_at`) AS `emitted_at`
from
    `joined_parse`
group by
    `joined_parse`.`house`,
    minute(`joined_parse`.`created_at`);
