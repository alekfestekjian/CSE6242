$(document).ready(function() {

    function UpdateSentChart(ticker) {
        let from_date = $('#from_date_picker').val() != '' ? $('#from_date_picker').val() : '2017-01-01';
        let to_date = $('#to_date_picker').val() != '' ? $('#to_date_picker').val() : '2022-09-30';

        let stockNm={'stockchoice':ticker, 'from_date': from_date, 'to_date': to_date};
        // $('#spinner').show();

        $.ajax({
            type: 'POST',
            url: '/getstock',
            data: JSON.stringify(stockNm),
            datatype: 'json',
            contentType: 'application/json; charset=UTF-8',
            success: function(data) {
                // $('#spinner').hide();
                $('#line-cht').show();
                let xDates = convertUtc(data['stockdata'].businessdate);
                let pPrices = data['stockdata'].predict_stdclose;
                let ePrices = data['stockdata'].ticker_stdclose;
                let sPrices = data['stockdata'].snp_stdclose;
                let dPrices = data['stockdata'].dji_stdclose;
                
                cht.PricingCht(ticker, xDates, ePrices, sPrices, dPrices, pPrices);
            }
        })
    }


    //find user sentiment data for the user selected ticker and date range
    //requires both date fields to be populated
    $('#main-search').click(function() {
        let ticker = $('#stocks option:selected').val();
        let from_date = $('#from_date_picker').val();
        let to_date = $('#to_date_picker').val();

        if (ticker===undefined) {
            alert("Ticker must be selected!")
            return;
        }

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

                    cht.SentimentCht(data['categorical'].category, data['categorical'].category_cnts, data['ticker'], data['from_date'], data['to_date'])
                                                                       
                }
            })

            //reload the chart with the updated date range if applicable
            UpdateSentChart($('#stocks').val());

        } else {
            alert('Both Dates Must Be Populated To Proceed!')
            return;
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
        constructor() {}

        PricingCht(ticker, labels, eqPrices, sPrices, dPrices, pPrices) {
            pricingchart.data.labels = labels;
            pricingchart.options.plugins.title.text = `Social Media Sentiment Analysis For ${ticker}`
            pricingchart.data.datasets[0].data = eqPrices;
            pricingchart.data.datasets[0].label = `Closing Prices For ${ticker}`;
            pricingchart.data.datasets[1].data = sPrices;
            pricingchart.data.datasets[1].label = 'Closing Prices For S&P 500';
            pricingchart.data.datasets[2].data = dPrices;
            pricingchart.data.datasets[2].label = 'Closing Prices For Dow Jones';
            pricingchart.data.datasets[3].data = pPrices;
            pricingchart.data.datasets[3].label = `Predicted Price For ${ticker}`;
            pricingchart.update();
        }
        SentimentCht(labels, data, ticker, from, to) {  
            sentimentchart.options.plugins.title.text = `User Sentiment For ${ticker}: ${convertUtc([from])} to ${convertUtc([to])}`
            sentimentchart.data.labels = labels;
            sentimentchart.data.datasets[0].data = data;
            sentimentchart.update();
        }
    }

    const linectx = document.getElementById('line-cht').getContext('2d');
    const barctx = document.getElementById('bar-cht').getContext('2d');

    const pricingchart = new Chart(linectx, {
        type: 'line',
        data: {
            datasets: [
                { borderColor: '#FF5733', fill: true },
                { borderColor: '#3DB700', fill: true },
                { borderColor: '#E8FF03', fill: true },
                { borderColor: '#034FFF', fill: true }
            ]
        },
        options: {
            responsive: true,
            maintainAspectRatio: true,
            plugins: { title: { display: true, text: '' } },
            scales: { x: { display: true  },  y: { display: true } }        
        }

    })

    const sentimentchart = new Chart(barctx, {
        type: 'bar',
        data: {
            datasets: [
                {   
                    backgroundColor: ["#3e95cd", "#8e5ea2","#3cba9f","#e8c3b9","#c45850"],
                    data: [],
                    fill: true
                }
            ]
        },
        options: {
            responsive: true,
            maintainAspectRatio: false,
            plugins: { title: { display: true, text: '' },
                       legend: { display: false }
            },
            scales: { y: { ticks: { precision: 0 } } }
        }

    })

    var cht = new ChartHandle();

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

    $('#stocks').change(function(e){
        UpdateSentChart(e.target.value);
    });

    //load with apple so the initial template isnt empty
    // UpdateSentChart('AAPL');

});