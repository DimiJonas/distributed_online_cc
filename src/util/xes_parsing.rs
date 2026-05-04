use crate::types::{XesAttributeValue, XesEvent};
use chrono::{DateTime, Utc};
use quick_xml::Reader;
use quick_xml::events::{BytesStart, Event};
use std::collections::HashMap;

pub fn parse_xes(data: &[u8]) -> Vec<XesEvent> {
    let mut reader = Reader::from_reader(data);
    reader.config_mut().trim_text(true);

    let mut events = Vec::new();
    let mut current_trace_id = String::new();
    let mut current_attributes: Option<HashMap<String, XesAttributeValue>> = None;
    let mut in_trace = false;

    loop {
        // event driven parsing
        match reader.read_event() {
            Ok(Event::Start(e)) | Ok(Event::Empty(e)) => match e.name().as_ref() {
                b"trace" => {
                    in_trace = true;
                    current_trace_id = String::new();
                }
                b"event" => {
                    current_attributes = Some(HashMap::new());
                }
                b"string" | b"date" | b"int" | b"float" | b"boolean" => {
                    let key = get_attribute(&e, b"key").unwrap_or_default();
                    let value = get_attribute(&e, b"value").unwrap_or_default();

                    if let Some(ref mut attributes) = current_attributes {
                        // inside event -> event attributes
                        let parsed_value = parse_attribute_value(e.name().as_ref(), &value);
                        attributes.insert(key, parsed_value);
                    } else if in_trace && key == "concept:name" {
                        // trace level attribute and concept:name = trace id
                        current_trace_id = value;
                    }
                }
                _ => {}
            },
            Ok(Event::End(e)) => match e.name().as_ref() {
                b"event" => {
                    if let Some(attributes) = current_attributes.take() {
                        events.push(XesEvent {
                            trace_id: current_trace_id.clone(),
                            attributes,
                        })
                    }
                }
                b"trace" => {
                    in_trace = false;
                }
                _ => {}
            },
            Ok(Event::Eof) => break,
            Err(e) => {
                eprintln!("Error parsing XES: {}", e);
                break;
            }
            _ => {}
        }
    }

    events
}

fn get_attribute(e: &BytesStart, name: &[u8]) -> Option<String> {
    e.attributes()
        .filter_map(|a| a.ok())
        .find(|a| a.key.as_ref() == name)
        .map(|a| String::from_utf8_lossy(&a.value).into_owned())
}

fn parse_attribute_value(tag: &[u8], value: &str) -> XesAttributeValue {
    match tag {
        b"date" => DateTime::parse_from_rfc3339(value)
            .map(|dt| XesAttributeValue::Date(dt.with_timezone(&Utc)))
            .unwrap_or(XesAttributeValue::String(value.to_string())),
        b"int" => value
            .parse::<i64>()
            .map(XesAttributeValue::Int)
            .unwrap_or(XesAttributeValue::String(value.to_string())),
        b"float" => value
            .parse::<f64>()
            .map(XesAttributeValue::Float)
            .unwrap_or(XesAttributeValue::String(value.to_string())),
        b"boolean" => XesAttributeValue::Boolean(value == "true"),
        _ => XesAttributeValue::String(value.to_string()),
    }
}
