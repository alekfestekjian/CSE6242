$(document).ready(function() {

    $('#stocks').change(function(e){
        
        let stockNm={'stockchoice':e.target.value};
        $('#hide-container').show()
        $('#hide-search').show()
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
                let ePrices = data['stockdata'].stdclose;
                let sPrices = data['snpdata'].stdclose;
                let dPrices = data['djidata'].stdclose;

                cht.LineChart(e.target.value, xDates, ePrices, sPrices, dPrices);
            }
        })

    });

    var from_date_bool=false, to_date_bool=false
    var from_date, to_date;

    //choose a from and to date
    $(document).on('click', '.datepicker', function(e) {
        $(`#${e.target.id}`).datepicker({
            onselect: function(d) {
                if (e.target.id=='from_date_picker') {
                    from_date_bool=true;
                    from_date=d;
                } else {
                    to_date_bool=true;
                    to_date=d;
                }
            }
        });

        $(`#${e.target.id}`).datepicker("show");
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