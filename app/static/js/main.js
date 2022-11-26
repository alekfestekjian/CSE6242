$(document).ready(function() {

    $('#stocks').change(function(e){
        
        let from_date = $('#from_date_picker').val() != '' ? $('#from_date_picker').val() : '2017-01-01';
        let to_date = $('#to_date_picker').val() != '' ? $('#to_date_picker').val() : '2022-09-30';

        let stockNm={'stockchoice':e.target.value, 'from_date': from_date, 'to_date': to_date};
        // $('#spinner').show();

        $.ajax({
            type: 'POST',
            url: '/getstock',
            data: JSON.stringify(stockNm),
            datatype: 'json',
            contentType: 'application/json; charset=UTF-8',
            success: function(data) {
                // $('#spinner').hide();
                let xDates = convertUtc(data['stockdata'].businessdate);
                let ePrices = data['stockdata'].ticker_stdclose;
                let sPrices = data['stockdata'].snp_stdclose;
                let dPrices = data['stockdata'].dji_stdclose;

                console.log(data); //why is bitcoin the only one cutting off the index pricing data around April...

                cht.LineChart(e.target.value, xDates, ePrices, sPrices, dPrices);
            }
        })

    });

    //choose a from and to date
    $(document).on('click', '.datepicker', function(e) {
        $(`#${e.target.id}`).datepicker({
            dateFormat: 'yy-mm-dd',
            defaultDate:"2022-09-30",
            // onselect: function(d) {
            //     if (e.target.id=='from_date_picker') {     
            //          considering disabling the date search beyond 9/2022 given our scope for this project                                  
            //     }
            // }
        });
        $(`#${e.target.id}`).datepicker("show");
    });

    //find user sentiment data for the user selected ticker and date range
    //requires both date fields to be populated
    $('#main-search').click(function() {
        let ticker = $('#stocks option:selected').val();
        let from_date = $('#from_date_picker').val();
        let to_date = $('#to_date_picker').val();

        if ( (from_date!=='') && (to_date!=='') ) {
           
            sentimentinfo = {
                'ticker': ticker,
                'from_date': from_date,
                'to_date': to_date
            }

            $.ajax({
                type: 'POST',
                url: '/getsentiment',
                data: JSON.stringify(sentimentinfo),
                datatype: 'json',
                contentType: 'application/json; charset=UTF-8',
                success: function(data) {
                    let reddit_stream="";
                    let comment_len=data['sentiment'].merged_comments.length;

                    for (let i=0; i<comment_len; i++) {
                        reddit_stream=reddit_stream.concat(data['sentiment'].merged_comments[i]);
                    }

                    $('#reddit-flow > marquee').html(reddit_stream);   
                }
            })

        } else {
            alert('Both Dates Must Be Populated To Proceed!')
        }
    });
    
    //need to convert the utc timestamps returned from our API
    function convertUtc(dates) {
        let dateLen=dates.length;
        let datelist=[];

        function padMoDy(num) {
            return num.toString().padStart(2,'0')
        }

        for (let i=0; i<dateLen; i++) {
            let iDate = new Date(dates[i]);

            let mo = padMoDy(iDate.getUTCMonth() + 1);
            let dy = padMoDy(iDate.getUTCDate());
            let yr = iDate.getUTCFullYear();

            datelist.push([mo, dy, yr].join('/')) ;
        }

        return datelist;
    }

    class ChartHandle {
        constructor(location) {
            this.location=location
        }

        LineChart(ticker, labels, eqPrices, sPrices, dPrices) {
            pricingchart.data.labels = labels;
            pricingchart.data.datasets[0].data = eqPrices;
            pricingchart.data.datasets[0].label = `Closing Prices For ${ticker}`;
            pricingchart.data.datasets[1].data = sPrices;
            pricingchart.data.datasets[1].label = 'Closing Prices For S&P 500';
            pricingchart.data.datasets[2].data = dPrices;
            pricingchart.data.datasets[2].label = 'Closing Prices For Dow Jones';
            pricingchart.update();
        }

    }

    const ctx = document.getElementById('line-cht').getContext('2d');

    const pricingchart = new Chart(ctx, {
        type: 'line',
        data: {
            labels: this.labels,
            datasets: [
                {
                    borderColor: '#FF5733',
                    fill: false
                }
                ,
                {
                    borderColor: '#3DB700',
                    fill: false
                }
                ,
                {
                    borderColor: '#E8FF03',
                    fill: false
                }

            ]
        },
        options: {
            responsive: true,
            maintainAspectRation: false,
            title: {
                display: true,
                text: 'Reddit Sentiment Analysis'
            },
            scales: {
                x: {
                    display: true
                },
                y: {
                    display: true
                }
            }
        }

    })

    var cht = new ChartHandle();

});