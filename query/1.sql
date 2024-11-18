--Qual a média de valor total (total_amount) recebido em um mês considerando todos os yellow taxis da frota.
with
	MONTHLY_AMOUNT AS(
		select 
			date_trunc('month',T."TPEP_PICKUP_DATETIME") as pickup_month
			,sum(T."TOTAL_AMOUNT") as total_amount
		from gold.f_yellow_taxi t
		group by 1
		order by 1
	)
select 
	avg(TOTAL_AMOUNT)
from MONTHLY_AMOUNT;