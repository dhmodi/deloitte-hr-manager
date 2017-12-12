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
				url: baseUrl + "query?v=20150910",
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
		   console.log("scrolled");
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
	  })
   
		});

function loadVoices() {
	// Fetch the available voices.
	var voices = speechSynthesis.getVoices();
	console.log("list to of voices ");
console.log(voices);
	// Loop through each of the voices.
	$('.input-field select').html("");
	voices.forEach(function (voice, i) {
		// Create a new option element.
		console.log(voice);
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
var accessToken = "872500f5983f46568df07d5ab0305eed";
var baseUrl = "https://api.api.ai/v1/";
var messages = [], //array that hold the record of each string in chat
lastUserMessage = "", //keeps track of the most recent input string from the user
botMessage = "", //var keeps track of what the chatbot is going to say
botName = 'Dr. Who'; //name of the chatbot

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
	console.log("stop recognition")
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
	console.log("inside of thie record");
	console.log(recognition);
	// $("#rec").text(recognition ? "Stop" : "Speak");
	image_url = (recognition ? "mic" : "mic_off");
	$("#rec .small")[0].innerText=image_url;
}

function send() {
	console.log("finally send came");
	var text = lastUserMessage;
	$.ajax({
		type: "POST",
		url: baseUrl + "query?v=20150910",
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
				 console.log("value is "+utterThis.lang);
				  utterThis.name = $(data)[0].innerHTML;
			  }
			 
			});
			console.log(utterThis);
         
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

