$(document).ready(function() {
    
    function UpdateSentimentData(ticker, from_date, to_date) {
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
                //populate market beta and predicted prices
                $('#bar-cht').show();                                               
            }
        })

    }

    function UpdateRecommendation(ticker, prices, dji, snp) {
        $('#recommend-tbl').show();
        $("#recommend-tbl tr").remove();

        let priceLen = prices['ticker'].length;
        let busdates = convertUtc(prices['businessdate']);

        $('#recommend-tbl').html('<tr><th>Date</th><th>Ticker</th><th>Predicted Price</th></tr>')

        for (let i=0; i<priceLen; i++) 
        {
            let row_str=`<tr class='rec-row'><td>${busdates[i]}</td><td>${ticker}</td><td>${prices['ticker_close'][i]}</td></tr>`
            $('#recommend-tbl tr:last').after(row_str);
        }

        $('#snp-beta').html(`Market beta for ${ticker} relative to the S&P 500 index:<br><span style='font-weight: bold; color:yellow;'>${snp}</span>`);
        $('#dji-beta').html(`Market beta for ${ticker} relative to the Dow Jones index:<br><span style='font-weight: bold; color:yellow;'>${dji}</span>`);

        return;
    }

    function UpdateSentChart(ticker) {
        let from_date = $('#from_date_picker').val() != '' ? $('#from_date_picker').val() : '2020-01-01';
        let to_date = $('#to_date_picker').val() != '' ? $('#to_date_picker').val() : '2020-02-28';
        let stockNm={'stockchoice':ticker, 'from_date': from_date, 'to_date': to_date};
        // $('#spinner').show();

        if (new Date(to_date) > new Date('2022-09-30')) 
            alert('Max Date Is September 30, 2022') 


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
                
                //market beta and predicted pricing data
                UpdateRecommendation(ticker, data['prediction'], data['dji_beta'], data['snp_beta'])
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
        
            //reload the chart with the updated date range if applicable
            UpdateSentChart($('#stocks').val());

            //load sentiment recommendation chart
            UpdateSentimentData(ticker, from_date, to_date);

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
            pricingchart.data.datasets[0].label = `${ticker}`;
            pricingchart.data.datasets[1].data = sPrices;
            pricingchart.data.datasets[1].label = 'S&P 500';
            pricingchart.data.datasets[2].data = dPrices;
            pricingchart.data.datasets[2].label = 'Dow Jones';
            pricingchart.data.datasets[3].data = pPrices;
            pricingchart.data.datasets[3].label = `${ticker} Predicted Price`;
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
            maintainAspectRatio: false,
            plugins: { title: { display: true, text: '', font: { size: 18 } } },
            scales: { x: { title: { display: true, text: 'Business Dates', font: { size: 15 } }} }        
        }

    })

    const sentimentchart = new Chart(barctx, {
        type: 'bar',
        data: {
            datasets: [
                {   
                    backgroundColor: ["#FF0013", "#FF0068","#FBFF06","#75F33E","#04FF00"],
                    data: [],
                    fill: true
                }
            ]
        },
        options: {
            responsive: true,
            maintainAspectRatio: false,
            plugins: { title: { display: true, text: '', color: "#fff" },
                       legend: { display: false }
            },
            scales: { y: { ticks: { precision: 0, color: "#fff" } }, x: { ticks: { color: "#fff" } }  }
        }

    })

    var cht = new ChartHandle();

    //choose a from and to date
    $(document).on('click', '.datepicker', function(e) {

        let max_dt = new Date('2022-09-30');
        let min_dt = new Date('2017-01-01');

        $(`#${e.target.id}`).datepicker({
            minDate: min_dt,
            maxDate: max_dt,
            dateFormat: 'yy-mm-dd',
            defaultDate:"2020-01-01",
            // onselect: function(d) {
            //     if (e.target.id=='from_date_picker') {     
            //          considering disabling the date search beyond 9/2022 given our scope for this project                                  
            //     }
            // }
        });
        $(`#${e.target.id}`).datepicker("show");
    });

    // $('#stocks').change(function(e){
    //     UpdateSentChart(e.target.value);
    // });

    //load with apple so the initial template isnt empty
    //comment later after testing
    UpdateSentChart('AAPL');
    UpdateSentimentData('AAPL', '2020-01-01', '2020-02-28');

});