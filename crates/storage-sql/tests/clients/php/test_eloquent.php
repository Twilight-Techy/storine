<?php
// Laravel's database layer (Eloquent ORM, schema builder, query builder) against storine-sql.
//
//   composer require illuminate/database:^11    (in a scratch directory)
//   php test_eloquent.php /path/to/vendor/autoload.php [pg_port] [mysql_port]

declare(strict_types=1);

require $argv[1] ?? __DIR__ . '/vendor/autoload.php';

use Illuminate\Database\Capsule\Manager as Capsule;
use Illuminate\Database\Eloquent\Model;
use Illuminate\Database\QueryException;
use Illuminate\Database\Schema\Blueprint;

$capsule = new Capsule();
$capsule->addConnection(['driver' => 'pgsql', 'host' => '127.0.0.1', 'port' => (int)($argv[2] ?? 55432), 'database' => 'shop', 'username' => 'app', 'password' => 'x'], 'pgsql');
$capsule->addConnection(['driver' => 'mysql', 'host' => '127.0.0.1', 'port' => (int)($argv[3] ?? 53306), 'database' => 'shop', 'username' => 'root', 'password' => ''], 'mysql');
$capsule->setAsGlobal();
$capsule->bootEloquent();

class Author extends Model
{
    protected $table = 'lv_authors';
    protected $guarded = [];
    protected $casts = ['royalties' => 'decimal:2', 'meta' => 'array'];

    public function books()
    {
        return $this->hasMany(Book::class, 'author_id');
    }
}

class Book extends Model
{
    protected $table = 'lv_books';
    protected $guarded = [];

    public function author()
    {
        return $this->belongsTo(Author::class, 'author_id');
    }
}

function check(bool $ok, string $what): void
{
    if (!$ok) {
        fwrite(STDERR, "FAILED: $what\n");
        exit(1);
    }
}

foreach (['pgsql', 'mysql'] as $conn) {
    Author::resolveConnection($conn);
    $capsule->getDatabaseManager()->setDefaultConnection($conn);
    $schema = Capsule::schema($conn);
    $schema->dropIfExists('lv_books');
    $schema->dropIfExists('lv_authors');
    $schema->create('lv_authors', function (Blueprint $t) {
        $t->id();
        $t->string('name')->unique();
        $t->decimal('royalties', 12, 2)->default(0);
        $t->json('meta')->nullable();
        $t->timestamps();
    });
    $schema->create('lv_books', function (Blueprint $t) {
        $t->id();
        $t->foreignId('author_id')->constrained('lv_authors')->cascadeOnDelete();
        $t->string('title');
        $t->integer('stock')->default(0);
        $t->timestamps();
        $t->index(['author_id', 'title']);
    });
    check($schema->hasTable('lv_books') && $schema->hasColumn('lv_books', 'stock'), "$conn: schema introspection");
    $schema->table('lv_books', function (Blueprint $t) {
        $t->string('isbn', 20)->nullable();
    });
    check($schema->hasColumn('lv_books', 'isbn'), "$conn: alter table");

    $ada = Author::create(['name' => 'Ada', 'royalties' => '10.50', 'meta' => ['tier' => 'gold']]);
    $ada->books()->createMany([['title' => 'One'], ['title' => 'Two', 'stock' => 3]]);
    Author::create(['name' => 'Bob']);

    $a = Author::with('books')->where('name', 'Ada')->first();
    check($a->books->count() === 2, "$conn: eager load");
    check($a->royalties === '10.50', "$conn: decimal cast {$a->royalties}");
    check($a->meta === ['tier' => 'gold'], "$conn: json cast");
    $counts = Author::withCount('books')->orderBy('name')->pluck('books_count', 'name')->all();
    check($counts == ['Ada' => 2, 'Bob' => 0], "$conn: withCount " . json_encode($counts));
    check((int)Book::sum('stock') === 3, "$conn: sum");
    check(Book::whereHas('author', fn ($q) => $q->where('name', 'like', 'A%'))->count() === 2, "$conn: whereHas");
    $page = Book::orderBy('title')->forPage(2, 1)->get();
    check(Book::count() === 2 && $page->first()->title === 'Two', "$conn: forPage");

    Book::where('title', 'Two')->increment('stock', 5);
    check(Book::where('title', 'Two')->value('stock') == 8, "$conn: increment");
    Capsule::connection($conn)->transaction(function () use ($ada) {
        $ada->books()->update(['stock' => 100]);
        $locked = Author::lockForUpdate()->find($ada->id);
        $locked->royalties = '20.00';
        $locked->save();
    });
    check(Author::find($ada->id)->royalties === '20.00', "$conn: transaction commit");
    try {
        Capsule::connection($conn)->transaction(function () use ($ada) {
            Author::where('id', $ada->id)->update(['royalties' => 0]);
            throw new RuntimeException('abort');
        });
    } catch (RuntimeException) {
    }
    check(Author::find($ada->id)->royalties === '20.00', "$conn: transaction rollback");
    try {
        Author::create(['name' => 'Ada']);
        check(false, "$conn: duplicate accepted");
    } catch (QueryException $e) {
        check(in_array($e->getCode(), ['23000', '23505'], true), "$conn: duplicate code {$e->getCode()}");
    }
    Author::updateOrCreate(['name' => 'Bob'], ['royalties' => '1.00']);
    check(Author::where('name', 'Bob')->value('royalties') == '1.00', "$conn: updateOrCreate");
    $ada->delete();
    check(Book::count() === 0, "$conn: cascade delete");
    $schema->drop('lv_books');
    $schema->drop('lv_authors');
    echo "OK  Eloquent ($conn)\n";
}
echo "all eloquent tests passed\n";
