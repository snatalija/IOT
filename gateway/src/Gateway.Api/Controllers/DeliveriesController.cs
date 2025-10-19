using Microsoft.AspNetCore.Mvc;
using Grpc.Net.Client;
using Gateway.Api.Models;

using Delivery;

namespace Gateway.Api.Controllers;

[ApiController]
[Route("api/deliveries")]
public class DeliveriesController : ControllerBase
{
    private readonly DeliveryService.DeliveryServiceClient _client;

    public DeliveriesController(IConfiguration cfg)
    {
        var addr = cfg.GetValue<string>("Datamanager:GrpcUrl") ?? "http://localhost:50051";
        var ch = GrpcChannel.ForAddress(addr);
        _client = new DeliveryService.DeliveryServiceClient(ch);
    }

    [HttpPost]
    public async Task<ActionResult<DeliveryDto>> Create([FromBody] DeliveryDto dto)
    {
        var req = new CreateRequest { Item = ToPb(dto) };
        var res = await _client.CreateAsync(req);
        return Ok(ToDto(res.Item));
    }

    [HttpGet("{id}")]
    public async Task<ActionResult<DeliveryDto>> GetById(string id)
    {
        var res = await _client.GetByIdAsync(new GetByIdRequest { Id = id });
        if (res.Item is null) return NotFound();
        return Ok(ToDto(res.Item));
    }

    [HttpPut("{id}")]
    public async Task<ActionResult<DeliveryDto>> Update(string id, [FromBody] DeliveryDto dto)
    {
        dto.Id = id;
        var res = await _client.UpdateAsync(new UpdateRequest { Item = ToPb(dto) });
        if (res.Item is null) return NotFound();
        return Ok(ToDto(res.Item));
    }

    [HttpDelete("{id}")]
    public async Task<IActionResult> Delete(string id)
    {
        var res = await _client.DeleteAsync(new DeleteRequest { Id = id });
        return res.Success ? NoContent() : NotFound();
    }

    [HttpGet]
    public async Task<ActionResult<IEnumerable<DeliveryDto>>> List(
        [FromQuery] string? city,
        [FromQuery] string? personId,
        [FromQuery] string? status,
        [FromQuery] DateTimeOffset? fromTs,
        [FromQuery] DateTimeOffset? toTs,
        [FromQuery] int limit = 50,
        [FromQuery] int offset = 0)
    {
        var req = new ListRequest
        {
            Filter = new QueryFilter
            {
                City = city ?? "",
                PersonId = personId ?? "",
                Status = status ?? "",
                FromTs = fromTs?.ToString("o") ?? "",
                ToTs = toTs?.ToString("o") ?? ""
            },
            Limit = limit,
            Offset = offset
        };
        var res = await _client.ListAsync(req);
        return Ok(res.Items.Select(ToDto));
    }

    public class AggregateQuery
    {
        public string FieldName { get; set; } = default!; // distance_km | time_taken_min
        public string Op { get; set; } = default!;        // MIN | MAX | AVG | SUM
    }

    [HttpPost("aggregate")]
    public async Task<ActionResult<object>> Aggregate(
        [FromBody] List<AggregateQuery> fields,
        [FromQuery] string? city,
        [FromQuery] string? personId,
        [FromQuery] string? status,
        [FromQuery] DateTimeOffset? fromTs,
        [FromQuery] DateTimeOffset? toTs)
    {
        var req = new AggregateRequest
        {
            Filter = new QueryFilter
            {
                City = city ?? "",
                PersonId = personId ?? "",
                Status = status ?? "",
                FromTs = fromTs?.ToString("o") ?? "",
                ToTs = toTs?.ToString("o") ?? ""
            }
        };
        req.Fields.AddRange(fields.Select(f => new AggregateField
        {
            FieldName = f.FieldName,
            Op = Enum.Parse<AggregateOp>(f.Op, ignoreCase: true)
        }));

        var res = await _client.AggregateAsync(req);
        return Ok(res.Results.Select(r => new { field = r.FieldName, op = r.Op.ToString(), value = r.Value }));
    }

    private static DeliveryDto ToDto(Delivery.Delivery d) => new DeliveryDto
    {
        Id = d.Id,
        OrderId = d.OrderId,
        DeliveryPersonId = d.DeliveryPersonId,
        City = d.City,
        Weather = d.Weather,
        Traffic = d.Traffic,
        DistanceKm = d.DistanceKm,
        TimeTakenMin = d.TimeTakenMin,
        DeliveryTimestamp = DateTimeOffset.Parse(d.DeliveryTimestamp),
        DeliveryStatus = d.DeliveryStatus
    };

    private static Delivery.Delivery ToPb(DeliveryDto dto) => new Delivery.Delivery
    {
        Id = dto.Id ?? "",
        OrderId = dto.OrderId,
        DeliveryPersonId = dto.DeliveryPersonId,
        City = dto.City,
        Weather = dto.Weather,
        Traffic = dto.Traffic,
        DistanceKm = dto.DistanceKm,
        TimeTakenMin = dto.TimeTakenMin,
        DeliveryTimestamp = dto.DeliveryTimestamp.ToString("o"),
        DeliveryStatus = dto.DeliveryStatus
    };
}
