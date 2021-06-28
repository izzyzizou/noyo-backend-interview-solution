import logging
import time

from datetime import datetime, timedelta

from flask import abort, jsonify
from webargs.flaskparser import use_args

from marshmallow import Schema, fields

from service.server import app, db
from service.models import AddressSegment
from service.models import Person

from sqlalchemy import desc, or_


class GetAddressQueryArgsSchema(Schema):
    date = fields.Date(required=False, missing=datetime.utcnow().date())


class AddressSchema(Schema):
    class Meta:
        ordered = True

    street_one = fields.Str(required=True, max=128)
    street_two = fields.Str(max=128)
    city = fields.Str(required=True, max=128)
    state = fields.Str(required=True, max=2)
    zip_code = fields.Str(required=True, max=10)

    start_date = fields.Date(required=True)
    end_date = fields.Date(required=False)


@app.route("/api/persons/<uuid:person_id>/address", methods=["GET"])
@use_args(GetAddressQueryArgsSchema(), location="querystring")
def get_address(args, person_id):
    person = Person.query.get(person_id)
    if person is None:
        abort(404, description="person does not exist")
    elif len(person.address_segments) == 0:
        abort(404, description="person does not have an address, please create one")
    # address_segment = AddressSegment.query.filter_by(person_id=person_id).all()
    qs = args["date"]
    address_segment = AddressSegment.query.filter(AddressSegment.start_date <= qs,
                                                  or_(qs <= AddressSegment.end_date,
                                                      AddressSegment.end_date == None)).order_by(
        desc(AddressSegment.start_date)).first()
    app.logger.info(f"ADDRESS DATA {address_segment}")
    if address_segment is None:
        # address_segment = person.address_segments[-1]
        abort(404, description=f"Address with {args['date']} start date is not found.")
    return jsonify(AddressSchema().dump(address_segment))


@app.route("/api/persons/<uuid:person_id>/address", methods=["PUT"])
@use_args(AddressSchema())
def create_address(payload, person_id):
    person = Person.query.get(person_id)
    if person is None:
        abort(404, description="person does not exist")
    # If there are no AddressSegment records present for the person, we can go
    # ahead and create with no additional logic.
    elif len(person.address_segments) == 0:
        address_segment = AddressSegment(
            street_one=payload.get("street_one"),
            street_two=payload.get("street_two"),
            city=payload.get("city"),
            state=payload.get("state"),
            zip_code=payload.get("zip_code"),
            start_date=payload.get("start_date"),
            person_id=person_id,
        )
        db.session.add(address_segment)
        db.session.commit()
        db.session.refresh(address_segment)
    else:
        # TODO: Implementation
        # If there are one or more existing AddressSegments, create a new AddressSegment
        # that begins on the start_date provided in the API request and continues
        # into the future. If the start_date provided is not greater than most recent
        # address segment start_date, raise an Exception.
        # raise NotImplementedError()
        start_date = payload.get("start_date")
        get_previous_address = AddressSegment.query.filter_by(end_date=None,
                                                              person_id=person_id).first()
        get_previous_address.end_date = start_date
        if start_date <= get_previous_address.start_date:
            raise Exception("New address start date must be after previous address start date")
        address_segment = AddressSegment(
            street_one=payload.get("street_one"),
            street_two=payload.get("street_two"),
            city=payload.get("city"),
            state=payload.get("state"),
            zip_code=payload.get("zip_code"),
            start_date=payload.get("start_date"),
            person_id=person_id
        )
        db.session.add(address_segment)
        db.session.commit()
        db.session.refresh(address_segment)

    return jsonify(AddressSchema().dump(address_segment))
