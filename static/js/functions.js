$(document).ready(function() {
			 recordarray =[];
			dataresult ="";
		  
		    window.speechSynthesis.onvoiceschanged = function() {
               loadVoices();
            };
			$("#chatbox").keypress(function(event) {
				if (event.which == 13) {
					event.preventDefault();
				
					newEntry();
				}
			});
			$("#rec").click(function(event) {
				switchRecognition();
			});
		$("#event").click(function(event) {
	newEntry();
		$.ajax({
				type: "POST",
				url: baseUrl + "contexts?sessionId=241187",
				contentType: "application/json; charset=utf-8",
				dataType: "json",
				headers: {
					"Authorization": "Bearer " + accessToken
				},
				data: JSON.stringify([{ name: 'medication-followup', lifespan: 5 }]),
			});
	$.ajax({
				type: "POST",
				url: baseUrl + "query?v=20170712",
				contentType: "application/json; charset=utf-8",
				dataType: "json",
				headers: {
					"Authorization": "Bearer " + accessToken
				},
				data: JSON.stringify({ event: { name: 'prescription_event', data: { 'doctor': 'Dr SAM', disease: 'Cold', medicine: 'CROCIN COLD & FLUMAX TABLETS', days: '20' } }, timezone: 'America/New_York', lang: 'en', sessionId: '241187'}),

				success: function(data) {
						setResponse(JSON.stringify(data, undefined, 2));
					setAudioResponse(data);

				},
				error: function() {
					setResponse("Internal Server Error");
				}
			});
			});
			
 
 
	$('#modalopen').click(function() {
		$('.input-field .dropdown-content').on('mousewheel DOMMouseScroll' , function(e) { 
		  
			e.stopPropagation() 
		 });
		 $('.input-field .dropdown-content li').scroll(function(e){
			e.stopPropagation();
		})
	   
	  if($('#modal1').hasClass('slide-right')) {
		$('#modal1').addClass('slide-left', 1000 ,'easeOutBounce' );
		$('#modal1').removeClass('slide-right'); 
	  }
  }); 

      $(".grid-container .margin-top").click(function(){
		$('#modal1').removeClass('slide-left');
		$('#modal1').addClass('slide-right', 1000 ,'easeOutBounce'); 
	  });

	   //Socket io related code goes over here
	   var socket = io.connect('https://deloitte-inventory-manager.herokuapp.com');
	   socket.on('chartdata',function(data){
		   var chartdetail = JSON.parse(data);
		   var length = chartdetail.length;
		   var width = 600;
		   var height=300;
		   var chartdetail ;
		  
		//    $.each("chartdetail",function(index , value){
		// 	  console.log(value);
		// 	     // CreateChart(value);
		//    });
		chartdetail.forEach(function(val , index, theArray){				
			CreateChart(val);
			status = true;
		});
	
		function CreateChart(data){
			var chartdatum = data;	
			//  if(length == 1){
			// 	   width = 900;
			// 	   height= 500;
			//  }		
			FusionCharts.setCurrentRenderer('javascript');			
			FusionCharts.ready(function () {
			//	console.log(typeof chartdatum);	
				var visitChart = new FusionCharts({
					type: chartdatum.type,
					id:chartdatum.chartcontainer+"-",
					renderer : 'javascript',
					renderAt: chartdatum.chartcontainer,
					width: "620",
					height: "300",
					dataFormat: 'json',
					dataSource:{
						"chart": {
							"caption": chartdatum.caption,
							"subCaption": chartdatum.subCaption,
							"xAxisName": chartdatum.xAxisName,
							"yAxisName": chartdatum.yAxisName,
							"lineThickness" : "2",
							"exportEnabled":'1',
							"paletteColors" : "#009688",
							"baseFontColor" : "#333333",
							"baseFont" : "Helvetica Neue,Arial",
							"captionFontSize" : "14",
							"subcaptionFontSize" : "14",
							"subcaptionFontBold" : "0",
							"showBorder" : "0",
							"bgColor" : "#ffffff",
							"showShadow" : "0",
							"canvasBgColor" : "#ffffff",
							"canvasBorderAlpha" : "0",
							"divlineAlpha" : "100",
							"divlineColor" : "#999999",
							"divlineThickness" : "1",
							"divLineIsDashed" : "1",
							"divLineDashLen" : "1",
							"divLineGapLen" : "1",
							"showXAxisLine" : "1",
							"xAxisLineThickness" : "1",
							"xAxisLineColor" : "#999999",
							"showAlternateHGridColor" : "0"                
						},
						"data": chartdatum.source
					}
				});
			//	var chartObject= FusionCharts(chartdatum.id);
			//chartdetail.push( visitChart.getData());
			console.log(visitChart);
				visitChart.render();
				$('#chartshow')[0].scrollIntoView(true);
			});
		
		   
	   }
	   $(".panel-heading").css("display","inline-block");  
	 //  console.log(chartdetail); 
		});
		
	});
function loadVoices() {
	// Fetch the available voices.
	var voices = speechSynthesis.getVoices();
	
	// Loop through each of the voices.
	$('.input-field select').html("");
	voices.forEach(function (voice, i) {
		// Create a new option element.
	
		if(voice.lang == 'en-US'){

        $('.input-field select').append($('<option>', {
                value: voice.lang,
                text : voice.name
            }));
            }
		});
	//	$('.input-field select').trigger('contentChanged');
		$('select').material_select(function(){
			record = recordarray.pop();
			//setAudioResponse(dataresult , record);
		});
		// $('.input-field select').on('change',function(){
		// 	console.log("change of list ");
		// 	record = recordarray.pop();
		// 	setAudioResponse(dataresult , record);
		// })
}

var recognition;
nlp = window.nlp_compromise;
var accessToken = "bc42b399e5a845df99644df738b1522c";
var baseUrl = "https://api.dialogflow.com/v1/";
var messages = [], //array that hold the record of each string in chat
lastUserMessage = "", //keeps track of the most recent input string from the user
botMessage = "", //var keeps track of what the chatbot is going to say
botName = 'Inventory Manager'; //name of the chatbot

function startRecognition() {
	recognition = new webkitSpeechRecognition();
	recognition.onstart = function (event) {
		updateRec();
	};
	recognition.onresult = function (event) {
		var text = "";
		for (var i = event.resultIndex; i < event.results.length; ++i) {
			text += event.results[i][0].transcript;
		}
		setInput(text);
		stopRecognition();
	};
	// recognition.onend = function () {
	// 	stopRecognition();
	// };
	recognition.lang = "en-US";
	recognition.start();
	console.log(recognition);
}

function stopRecognition() {
	
	if (recognition) {
		recognition.stop();
		recognition = null;
	}
	updateRec();
}

function switchRecognition() {
	if (recognition) {
	
		stopRecognition();
	} else {
		
		startRecognition();
	}
}

function setInput(text) {
	$("#chatbox").val(text);
	// send();
	newEntry();
}

function updateRec() {

	// $("#rec").text(recognition ? "Stop" : "Speak");
	image_url = (recognition ? "mic" : "mic_off");
	$("#rec .small")[0].innerText=image_url;
}

function send() {
	
	var text = lastUserMessage;
	$.ajax({
		type: "POST",
		url: baseUrl + "query?v=20170712",
		contentType: "application/json; charset=utf-8",
		dataType: "json",
		headers: {
			"Authorization": "Bearer " + accessToken
		},
		data: JSON.stringify({
			query: text,
			lang: "en",
			sessionId: "241187"
		}),

		success: function (data) {
			setResponse(JSON.stringify(data, undefined, 2));
			setAudioResponse(data);
             dataresult = data;
		},
		error: function () {
			setResponse("Internal Server Error");
		}
	});
	setResponse("Loading...");
}

function setResponse(val) {
	$("#response").text(val);
}

function setAudioResponse(val ,record) {
	console.log("value of data  is "+val+" and record is "+ record);
	 // $("#response").text(val);
	if (val.result) {
		var say = "";
		say = val.result.fulfillment.messages;
		// botMessage = say
		for (var j = 0; j < say.length; j++) {
			botMessage = say[j].speech;

			messages.push("<b>" + botName + ":</b> " + botMessage);
			for (var i = 1; i < 11; i++) {
				if (messages[messages.length - i])
					$("#chatlog" + i).html(messages[messages.length - i]);
			}
			synth = window.speechSynthesis;
			var utterThis = new SpeechSynthesisUtterance(botMessage);
		//	utterThis.lang = $("#voiceSelect option:selected").val();
			if(!record){
				if($("ul li.active span")[0] == undefined){
					record =  $("ul li span")[0].innerHTML;
					recordarray.push(record);
				}
				else{
				  record =  $("ul li.active span")[0].innerHTML;
				  recordarray.push(record);
			  }
			}
		
			var counter = $("select option");
			$.map(counter,function(data){				
              if(record == $(data)[0].innerHTML){
				  utterThis.lang = $(data)[0].value;
				 
				  utterThis.name = $(data)[0].innerHTML;
			  }
			 
			});
		
			synth.speak(utterThis);
		
		}
	}
}
//this runs each time enter is pressed.
//It controls the overall input and output
function newEntry() {
	//if the message from the user isn't empty then run
	if ($("#chatbox").val() != "") {
		//pulls the value from the chatbox ands sets it to lastUserMessageS
		lastUserMessage = $("#chatbox").val();
		//sets the chat box to be clear
		$("#chatbox").val("");
		//adds the value of the chatbox to the array messages
		messages.push("<b>Me: </b>" + lastUserMessage);
		//Speech(lastUserMessage);  //says what the user typed outloud
		//sets the variable botMessage in response to lastUserMessage
		send();
		// botMessage = '';
		//add the chatbot's name and message to the array messages
		//messages.push("<b>" + botName + ":</b> " + botMessage);
		// says the message using the text to speech function written below
		//outputs the last few array elements of messages to html
		for (var i = 1; i < 11; i++) {
			if (messages[messages.length - i])
				$("#chatlog" + i).html(messages[messages.length - i]);
		}
	}
}

